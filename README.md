# PostgreSQL MCP Server

A Model Context Protocol (MCP) server that enables querying PostgreSQL databases.

I built this because the reference because clients had trouble with the reference Postgres MCP server when there were mulitple databases. This version passes through a user specified server name to the client.

Note: There are probably better implementations out there, but it's currently a pain to find them.

## Installation

```bash
npm install
npm run build
```

## Configuration

The server uses PostgreSQL connection strings. You can either:

1. Set the `DATABASE_URL` environment variable
2. Pass a `connectionString` parameter directly to the query tool

Connection string format: `postgresql://user:password@host:port/database`

### Environment Variables

#### MCP_SERVER_NAME
You can customize the MCP server name using the `MCP_SERVER_NAME` environment variable. This is useful when running multiple PostgreSQL connections to distinguish between them.

Example:
```bash
MCP_SERVER_NAME="postgres-production" npm start
```

#### READ_ONLY_MODE
Controls whether the server enforces read-only mode to prevent write operations. Defaults to `true` for security.

- `READ_ONLY_MODE=true` (default): Prevents INSERT, UPDATE, DELETE, CREATE, ALTER, DROP operations
- `READ_ONLY_MODE=false`: Allows all operations based on user permissions

## Usage


### Using with Claude Desktop

Add this to your Claude Desktop configuration:

```json
{
  "mcpServers": {
    "postgres": {
      "command": "node",
      "args": ["/path/to/postgres-mcp/dist/index.js"],
      "env": {
        "DATABASE_URL": "postgresql://myuser:mypassword@localhost:5432/mydb",
        "MCP_SERVER_NAME": "this-server-name"
      }
    }
  }
}
```

### Available Tools

#### query

Execute a PostgreSQL query.

Parameters:
- `query` (required): The SQL query to execute
- `connectionString` (optional): PostgreSQL connection string (uses DATABASE_URL env var if not provided)

Example:
```json
{
  "query": "SELECT * FROM users LIMIT 10"
}
```
