import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";
import path from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
import dotenv from "dotenv";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, ".env") });
const GATEWAY_ADDR = process.env.VIZAI_ADDR || "127.0.0.1:50051";
const CONNECTOR_ID = "poc-connector";
const DEMO_TOKEN = "viz-demo-token";
const DATABASE_URL = process.env.DATABASE_URL;
const DATABASE_SCHEMA = process.env.DATABASE_SCHEMA || "public";

if (!DATABASE_URL) {
  console.error("[node] set DATABASE_URL in node/.env");
  process.exit(1);
}

const packageDefinition = protoLoader.loadSync(
  path.join(__dirname, "..", "proto", "connector.proto"),
  { keepCase: false, longs: String, defaults: true, oneofs: true }
);
const proto = grpc.loadPackageDefinition(packageDefinition).viz.connector.v1;

const pool = new pg.Pool({
  connectionString: DATABASE_URL,
  max: 4,
  statement_timeout: 30000,
});
const seen = new Map();

function nowMs() {
  return Number(process.hrtime.bigint() / 1000000n);
}

const COUNT_KEYS = new Set(["tables", "columns", "rows", "bytes"]);

function logTiming(label, parts) {
  const detail = Object.entries(parts)
    .map(([name, value]) =>
      COUNT_KEYS.has(name) ? `${name}=${value}` : `${name}=${value}ms`
    )
    .join(" | ");
  console.log(`[node][timing] ${label} | ${detail}`);
}

function assertSingleRead(sql) {
  const text = String(sql || "").trim().replace(/;+\s*$/, "");
  if (!text) {
    throw new Error("empty SQL");
  }
  if (text.includes(";")) {
    throw new Error("only a single statement is allowed");
  }
  if (!/^(select|with)\b/i.test(text)) {
    throw new Error("only a single SELECT is allowed");
  }
  return text;
}

function cell(value) {
  if (value === null || value === undefined) {
    return "";
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

async function extractSchema() {
  const started = nowMs();
  const result = await pool.query(
    `SELECT table_name, column_name, data_type
     FROM information_schema.columns
     WHERE table_schema = $1
     ORDER BY table_name, ordinal_position`,
    [DATABASE_SCHEMA]
  );
  const catalogMs = nowMs() - started;
  const tables = new Map();
  for (const row of result.rows) {
    if (!tables.has(row.table_name)) {
      tables.set(row.table_name, { name: row.table_name, columns: [] });
    }
    tables.get(row.table_name).columns.push({
      name: row.column_name,
      type: row.data_type,
    });
  }
  const schema = {
    schemaName: DATABASE_SCHEMA,
    tables: [...tables.values()],
  };
  const assembleMs = nowMs() - started - catalogMs;
  logTiming("schema discovery", {
    catalog_query: catalogMs,
    assemble: assembleMs,
    total: nowMs() - started,
    tables: schema.tables.length,
    columns: result.rows.length,
  });
  return schema;
}

async function executeQuery(request) {
  const started = nowMs();
  const maxRows = request.maxRows > 0 ? request.maxRows : 100;
  const maxBytes = request.maxBytes > 0 ? request.maxBytes : 1000000;
  const acquired = nowMs();
  const client = await pool.connect();
  const poolWaitMs = nowMs() - acquired;
  let sqlMs = 0;
  let packMs = 0;
  try {
    const sql = assertSingleRead(request.sql);
    console.log(`[node] execute ${request.requestId} | ${sql}`);
    await client.query(`SET search_path TO ${client.escapeIdentifier(DATABASE_SCHEMA)}`);
    const sqlStarted = nowMs();
    const result = await client.query(sql);
    sqlMs = nowMs() - sqlStarted;
    const packStarted = nowMs();
    const names = result.fields.map((field) => field.name);
    let truncated = false;
    let chosen = result.rows;
    if (chosen.length > maxRows) {
      chosen = chosen.slice(0, maxRows);
      truncated = true;
    }
    const rows = [];
    let used = 0;
    for (const record of chosen) {
      const values = names.map((name) => cell(record[name]));
      const size = Buffer.byteLength(JSON.stringify(values));
      if (used + size > maxBytes) {
        truncated = true;
        break;
      }
      used += size;
      rows.push({ values });
    }
    packMs = nowMs() - packStarted;
    const totalMs = nowMs() - started;
    logTiming(`sql ${request.requestId}`, {
      pool_wait: poolWaitMs,
      sql_execution: sqlMs,
      pack_rows: packMs,
      total: totalMs,
      rows: rows.length,
      bytes: used,
    });
    return {
      requestId: request.requestId,
      columns: names.map((name) => ({ name, type: "" })),
      rows,
      rowCount: rows.length,
      truncated,
      executionTimeMs: String(totalMs),
      error: "",
    };
  } catch (err) {
    logTiming(`sql rejected ${request.requestId}`, {
      pool_wait: poolWaitMs,
      sql_execution: sqlMs,
      total: nowMs() - started,
    });
    console.log(`[node] rejected ${request.requestId} | ${err.message}`);
    return {
      requestId: request.requestId,
      columns: [],
      rows: [],
      rowCount: 0,
      truncated: false,
      executionTimeMs: String(nowMs() - started),
      error: err.message,
    };
  } finally {
    client.release();
  }
}

async function connect() {
  const boot = nowMs();
  await pool.query("SELECT 1");
  logTiming("database connect", { total: nowMs() - boot });
  console.log(`[node] database connected schema=${DATABASE_SCHEMA}`);

  const client = new proto.VizGateway(
    GATEWAY_ADDR,
    grpc.credentials.createInsecure()
  );
  const stream = client.connect();

  stream.on("data", async (message) => {
    const job = message.job;
    if (!job) {
      return;
    }
    const jobStarted = nowMs();
    if (seen.has(job.requestId)) {
      console.log(`[node] duplicate ${job.requestId}, not executed again`);
      stream.write({ result: seen.get(job.requestId) });
      return;
    }
    const result = await executeQuery(job);
    const sendStarted = nowMs();
    seen.set(job.requestId, result);
    stream.write({ result });
    logTiming(`send result ${job.requestId}`, {
      send: nowMs() - sendStarted,
      job_total: nowMs() - jobStarted,
    });
  });

  stream.on("error", (err) => {
    console.error("[node] stream error:", err.message);
    process.exit(1);
  });

  stream.on("end", () => {
    console.log("[node] VizAI closed the connection");
    process.exit(0);
  });

  const helloStarted = nowMs();
  stream.write({
    hello: { connectorId: CONNECTOR_ID, token: DEMO_TOKEN },
  });
  logTiming("send hello", { send: nowMs() - helloStarted, since_boot: nowMs() - boot });
  console.log(`[node] dialed VizAI at ${GATEWAY_ADDR}`);

  const schema = await extractSchema();
  const sendSchemaStarted = nowMs();
  stream.write({ schema });
  logTiming("send schema", {
    send: nowMs() - sendSchemaStarted,
    tables: schema.tables.length,
    since_boot: nowMs() - boot,
  });
  console.log(`[node] pushed schema: ${schema.tables.length} table(s)`);
}

connect().catch((err) => {
  console.error("[node] startup failed:", err.message);
  process.exit(1);
});
