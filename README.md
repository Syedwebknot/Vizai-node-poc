# Node connector POC

Node is the connector. It owns the database login, dials VizAI over gRPC, pushes schema, then runs the one SELECT VizAI generates. Python never opens the database. Rows are printed and not stored.

Copy [node/.env.example](node/.env.example) to `node/.env` and set `DATABASE_URL` and `DATABASE_SCHEMA`. Those values stay in Node.

## One-time setup

```powershell
cd viz-node-connector-poc\node
copy .env.example .env
npm install

cd ..\python
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
.\.venv\Scripts\python -m grpc_tools.protoc -I ..\proto --python_out=. --grpc_python_out=. ..\proto\connector.proto
```

## Run

Start Python first so port 50051 is listening.

```powershell
cd viz-node-connector-poc\python
.\.venv\Scripts\python gateway.py
```

```powershell
cd viz-node-connector-poc\node
node agent.js
```

On connect, Node pushes schema. Python generates one SELECT from that schema, Node executes it, and Python prints the rows. You can type more SELECTs after that. An empty line exits.
