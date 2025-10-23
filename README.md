# PostgreSQL MCP Server

A Model Context Protocol (MCP) server that exposes PostgreSQL over the Streamable HTTP transport.  
It manages per-session database connections, enforces optional read-only access, and adds
structured logging to help debug queries and connectivity issues.

## Installation

```bash
npm install
npm run build
```

## Running the server

```bash
# build (optional if you use `npm run dev`)
npm run build

# start the compiled server
node dist/index.js

# or run directly with hot reload while developing
npm run dev
```

By default the server listens on `http://localhost:3000/mcp`. Set `MCP_PORT` to override the port.

## Configuration

The server loads configuration from environment variables (via `.env`).  
Set the `DATABASE_URL` environment variable before starting the server so every tool call uses the same PostgreSQL instance.

Example `.env`:

```env
DATABASE_URL=postgresql://user:password@host:5432/database
MCP_SERVER_NAME=postgres-mcp
```

The server will refuse to start if `DATABASE_URL` is missing.

### Environment Variables

| Variable              | Default | Description |
| --------------------- | ------- | ----------- |
| `DATABASE_URL`        | —       | PostgreSQL connection string loaded at startup (required). |
| `MCP_SERVER_NAME`     | `postgres-mcp` | Label used in log messages and tool names. Helpful when you run multiple instances. |
| `MCP_READ_ONLY_MODE`* | `true`  | When truthy, enforces read-only sessions via `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY` and validates queries. Set to `false` to allow writes. (`READ_ONLY_MODE` is still honoured for backwards compatibility.) |
| `MCP_PORT`            | `3000`  | Streamable HTTP port. |

> \* Any non-empty value other than `"false"` keeps read-only mode enabled.

## HTTP interface

The server implements the Streamable HTTP transport:

| Method | Path       | Description                                                                                               |
| ------ | ---------- | --------------------------------------------------------------------------------------------------------- |
| POST   | `/mcp`     | Handles JSON-RPC requests. Initialization responses include a `Mcp-Session-Id` header for subsequent calls. |
| GET    | `/mcp`     | Establishes the server-sent event stream for notifications. Requires `mcp-session-id` header.             |
| DELETE | `/mcp`     | Terminates the session identified by `mcp-session-id`.                                                    |

Every session is isolated: database connections, query caches, and transport state are scoped to the generated session ID. Comprehensive logs are emitted to stdout/stderr with the pattern `[postgres-mcp] [http] ...` or `[postgres-mcp] [session:abc123] ...`.

## Client configuration

Point your MCP client at the HTTP endpoint. Example entry for `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "postgres": {
      "transport": {
        "type": "streamable-http",
        "url": "http://localhost:3000/mcp"
      }
    }
  }
}
```

Set environment variables for the server process (e.g., via `.env`, your process manager, or shell exports) before launching it.

## Tools

### Available Tools

#### query

Execute a PostgreSQL query.

Parameters:
- `query` (required): The SQL query to execute

Example:
```json
{
  "query": "SELECT * FROM users LIMIT 10"
}
```

All tools run against the database specified by `DATABASE_URL`.
