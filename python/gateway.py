"""Stand-in for VizAI.

The Node connector dials in, pushes schema, then this process generates
one SELECT from that schema, sends it, and prints the rows.
This process never opens the database.
"""

import queue
import sys
import threading
import time
import uuid
from concurrent import futures

import grpc

import connector_pb2 as pb
import connector_pb2_grpc as pb_grpc

GATEWAY_ADDR = "127.0.0.1:50051"
CONNECTOR_ID = "poc-connector"
DEMO_TOKEN = "viz-demo-token"
RESULT_TIMEOUT_SECONDS = 30
PREFERRED_TABLES = ("sales_data", "sales", "leases", "properties", "units")


def ms_since(started):
    return int((time.perf_counter() - started) * 1000)


def log_timing(label, parts):
    count_keys = {"tables", "columns", "rows"}
    detail = " | ".join(
        f"{name}={value}" if name in count_keys else f"{name}={value}ms"
        for name, value in parts.items()
    )
    print(f"[python][timing] {label} | {detail}", flush=True)


class Gateway(pb_grpc.VizGatewayServicer):
    def __init__(self):
        self._outgoing = queue.Queue()
        self._results = {}
        self._lock = threading.Lock()
        self.ready = threading.Event()
        self.schema_ready = threading.Event()
        self.schema = None
        self.connected_at = None
        self.schema_received_at = None

    def Connect(self, request_iterator, context):
        def read_agent():
            try:
                for message in request_iterator:
                    kind = message.WhichOneof("payload")
                    if kind == "hello":
                        hello = message.hello
                        if hello.token != DEMO_TOKEN or hello.connector_id != CONNECTOR_ID:
                            context.abort(grpc.StatusCode.UNAUTHENTICATED, "bad connector token")
                            return
                        self.connected_at = time.perf_counter()
                        print("[python] Node connector connected", flush=True)
                        self.ready.set()
                    elif kind == "schema":
                        self.schema_received_at = time.perf_counter()
                        self.schema = message.schema
                        wait_ms = (
                            ms_since(self.connected_at)
                            if self.connected_at is not None
                            else 0
                        )
                        print(
                            f"[python] received schema {message.schema.schema_name}: "
                            f"{len(message.schema.tables)} table(s)",
                            flush=True,
                        )
                        log_timing(
                            "receive schema",
                            {
                                "wait_after_hello": wait_ms,
                                "tables": len(message.schema.tables),
                            },
                        )
                        self.schema_ready.set()
                    elif kind == "result":
                        arrived = time.perf_counter()
                        with self._lock:
                            waiter = self._results.pop(message.result.request_id, None)
                        if waiter is not None:
                            waiter.put((message.result, arrived))
            finally:
                self.ready.clear()
                self.schema_ready.clear()

        threading.Thread(target=read_agent, daemon=True).start()
        while context.is_active():
            message = self._outgoing.get()
            if message is None:
                return
            yield message

    def execute(self, sql):
        request_id = str(uuid.uuid4())
        waiter = queue.Queue()
        with self._lock:
            self._results[request_id] = waiter
        sent = time.perf_counter()
        self._outgoing.put(
            pb.VizMessage(
                job=pb.ExecuteQueryRequest(
                    request_id=request_id,
                    connector_id=CONNECTOR_ID,
                    sql=sql,
                    timeout_seconds=RESULT_TIMEOUT_SECONDS,
                    max_rows=20,
                    max_bytes=1000000,
                )
            )
        )
        try:
            result, arrived = waiter.get(timeout=RESULT_TIMEOUT_SECONDS + 5)
        except queue.Empty:
            with self._lock:
                self._results.pop(request_id, None)
            raise TimeoutError(f"no result for {request_id}")
        sql_ms = int(result.execution_time_ms or 0)
        round_trip = ms_since(sent)
        log_timing(
            f"query round-trip {request_id}",
            {
                "send_and_wait": round_trip,
                "connector_sql": sql_ms,
                "grpc_overhead": max(round_trip - sql_ms, 0),
                "rows": result.row_count,
            },
        )
        return result


def generate_query(schema):
    tables = list(schema.tables)
    table = None
    for name in PREFERRED_TABLES:
        table = next((item for item in tables if item.name == name), None)
        if table:
            break
    if table is None and tables:
        table = tables[0]
    if table is None:
        raise ValueError("schema has no tables")

    schema_name = schema.schema_name
    qualified = f"{schema_name}.{table.name}" if schema_name else table.name
    columns = [column.name for column in table.columns]
    if "status" in columns:
        return (
            f"SELECT status, COUNT(*) AS row_count FROM {qualified} "
            "GROUP BY status ORDER BY row_count DESC LIMIT 20"
        )
    selected = ", ".join(columns[:6]) if columns else "*"
    return f"SELECT {selected} FROM {qualified} LIMIT 10"


def print_schema(schema):
    names = [table.name for table in schema.tables[:15]]
    extra = len(schema.tables) - len(names)
    print("tables:", ", ".join(names) + (f", ... (+{extra} more)" if extra > 0 else ""))


def print_result(result):
    if result.error:
        print(f"error: {result.error}")
        return
    headers = [column.name for column in result.columns]
    print(" | ".join(headers) if headers else "(no columns)")
    for row in result.rows:
        print(" | ".join(row.values))
    suffix = " truncated" if result.truncated else ""
    print(f"{result.row_count} row(s){suffix} in {result.execution_time_ms} ms")
    print()


def main():
    gateway = Gateway()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    pb_grpc.add_VizGatewayServicer_to_server(gateway, server)
    server.add_insecure_port(GATEWAY_ADDR)
    server.start()
    print(f"[python] waiting for the Node connector on {GATEWAY_ADDR}")
    print("[python] this process does not open the database")
    gateway.ready.wait()
    print("[python] waiting for schema push")
    if not gateway.schema_ready.wait(timeout=60):
        print("error: schema did not arrive")
        gateway._outgoing.put(None)
        server.stop(2)
        return
    print_schema(gateway.schema)
    generate_started = time.perf_counter()
    sql = generate_query(gateway.schema)
    log_timing("generate sql", {"total": ms_since(generate_started)})
    print(f"[python] generated query:\n{sql}")
    first_query_started = time.perf_counter()
    try:
        print_result(gateway.execute(sql))
        log_timing(
            "first result shown",
            {
                "query_to_print": ms_since(first_query_started),
                "since_connect": (
                    ms_since(gateway.connected_at)
                    if gateway.connected_at is not None
                    else 0
                ),
            },
        )
    except TimeoutError as exc:
        print(f"error: {exc}")
    print("Type another SELECT and press Enter. Empty line exits.")
    try:
        while True:
            extra = input("sql> ").strip()
            if not extra:
                break
            try:
                print_result(gateway.execute(extra))
            except TimeoutError as exc:
                print(f"error: {exc}")
            except grpc.RpcError as exc:
                print(f"error: {exc.details()}")
    except (EOFError, KeyboardInterrupt):
        print()
    finally:
        gateway._outgoing.put(None)
        server.stop(2)


if __name__ == "__main__":
    try:
        main()
    except grpc.RpcError as exc:
        print(f"error: {exc.details()}", file=sys.stderr)
        sys.exit(1)
