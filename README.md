# PostgreSQL MCP Server

A Model Context Protocol (MCP) server that enables querying PostgreSQL databases.

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

Example:
```bash
READ_ONLY_MODE=false npm start
```

See `.env.example` for an example configuration.

## Usage

### Running the server

```bash
npm start
```

### Using with Claude Desktop

Add this to your Claude Desktop configuration:

```json
{
  "mcpServers": {
    "postgres": {
      "command": "node",
      "args": ["/path/to/postgres-mcp/dist/index.js"],
      "env": {
        "DATABASE_URL": "postgresql://myuser:mypassword@localhost:5432/mydb"
      }
    }
  }
}
```

#### Multiple Database Connections

To connect to multiple PostgreSQL databases, configure each with a unique name:

```json
{
  "mcpServers": {
    "postgres-prod": {
      "command": "node",
      "args": ["/path/to/postgres-mcp/dist/index.js"],
      "env": {
        "MCP_SERVER_NAME": "postgres-production",
        "DATABASE_URL": "postgresql://user:pass@prod.example.com:5432/proddb"
      }
    },
    "postgres-dev": {
      "command": "node",
      "args": ["/path/to/postgres-mcp/dist/index.js"],
      "env": {
        "MCP_SERVER_NAME": "postgres-development",
        "DATABASE_URL": "postgresql://user:pass@localhost:5432/devdb"
      }
    }
  }
}
```

This will make the tools available as:
- `mcp__postgres-prod__query` for production database
- `mcp__postgres-dev__query` for development database

### Using with Claude Code

For Claude Code users, you can add the PostgreSQL MCP server using the following command:

```bash
# Single database connection
claude mcp add postgres-local node /path/to/postgres-mcp/dist/index.js -e DATABASE_URL=postgresql://user:pass@localhost:5432/mydb

# Multiple database connections with custom names
claude mcp add postgres-prod node /path/to/postgres-mcp/dist/index.js -e MCP_SERVER_NAME=postgres-production -e DATABASE_URL=postgresql://user:pass@prod.example.com:5432/proddb

claude mcp add postgres-dev node /path/to/postgres-mcp/dist/index.js -e MCP_SERVER_NAME=postgres-development -e DATABASE_URL=postgresql://user:pass@localhost:5432/devdb
```

#### Read-Only Analytics Database Example

For analytics or reporting databases where you want to prevent accidental writes:

```bash
# Add a read-only analytics database connection (read-only mode is enabled by default)
claude mcp add analytics-db node /path/to/postgres-mcp/dist/index.js \
  -e "MCP_SERVER_NAME=analytics-db" \
  -e "DATABASE_URL=postgresql://readonly_user:secure_password@analytics.example.com:5432/analytics_db" \
  -s user
```

This configuration:
- Uses a dedicated read-only database user
- Enables read-only mode by default (prevents write operations)
- Sets user scope for personal analytics access
- Provides safe access to sensitive analytics data

To allow write operations if needed, add `-e "READ_ONLY_MODE=false"` to the command.

To share the configuration with your team (project scope):
```bash
claude mcp add --scope project postgres node /path/to/postgres-mcp/dist/index.js -e DATABASE_URL=postgresql://user:pass@localhost:5432/mydb
```

For more information, see the [Claude Code MCP documentation](https://docs.anthropic.com/en/docs/claude-code/mcp).

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

## Development

```bash
npm run dev
```

This will start the server in watch mode, automatically recompiling when changes are detected.