#!/usr/bin/env node
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { ErrorCode, McpError } from '@modelcontextprotocol/sdk/types.js';
import type { LoggingLevel, TextContent } from '@modelcontextprotocol/sdk/types.js';
import { Client } from 'pg';
import { z } from 'zod';
import { QueryValidator } from './queryValidator.js';

const QueryInputShape = {
  query: z.string().describe('SQL to execute'),
  maxRows: z
    .number()
    .int()
    .positive()
    .max(1_000_000)
    .optional()
    .describe('Optional row limit applied server-side (defaults to 10)'),
  timeoutMs: z
    .number()
    .int()
    .positive()
    .max(600_000)
    .optional()
    .describe('Per-query statement timeout'),
};

const QueryInputSchema = z.object(QueryInputShape);

const SharedPredefinedInputShape = {
  maxRows: QueryInputShape.maxRows,
  timeoutMs: QueryInputShape.timeoutMs,
  schema: z
    .string()
    .optional()
    .describe('Limit results to a specific schema'),
};

const ViewSchemasInputShape = {
  ...SharedPredefinedInputShape,
  view: z
    .string()
    .optional()
    .describe('Limit results to a specific view name'),
};

const TableSchemasInputShape = {
  ...SharedPredefinedInputShape,
  table: z
    .string()
    .optional()
    .describe('Limit results to a specific table name'),
};

const ViewSchemasInputSchema = z.object(ViewSchemasInputShape);
const TableSchemasInputSchema = z.object(TableSchemasInputShape);

const QueryOutputShape = {
  rowCount: z.number(),
  rows: z.array(z.record(z.any())),
  fields: z
    .array(z.object({ name: z.string(), dataTypeID: z.number() }))
    .optional(),
};

const QueryOutputSchema = z.object(QueryOutputShape);

const VIEW_SCHEMAS_BASE = `
SELECT *
FROM pg_catalog.pg_views pv
WHERE pv.schemaname NOT IN ('pg_catalog', 'information_schema')
`.trim();

const TABLE_SCHEMAS_BASE_CTE = `
WITH table_columns AS (
  SELECT
    n.nspname AS schemaname,
    c.relname AS tablename,
    c.oid AS table_oid,
    a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
    CASE WHEN a.attnotnull THEN 'NOT NULL' ELSE '' END AS not_null,
    pg_get_expr(ad.adbin, ad.adrelid) AS default_value,
    a.attnum,
    col_description(c.oid, a.attnum) AS column_comment
  FROM pg_catalog.pg_attribute a
    JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    LEFT JOIN pg_catalog.pg_attrdef ad ON (a.attrelid = ad.adrelid AND a.attnum = ad.adnum)
  WHERE c.relkind = 'r'
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    AND a.attnum > 0
    AND NOT a.attisdropped
),
table_comments AS (
  SELECT
    n.nspname AS schemaname,
    c.relname AS tablename,
    obj_description(c.oid, 'pg_class') AS table_comment
  FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
  WHERE c.relkind = 'r'
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
)
SELECT
  tc.schemaname,
  tc.tablename,
  'CREATE TABLE ' || tc.schemaname || '.' || tc.tablename || E'(\\n' ||
  array_to_string(
    array_agg(
      '    ' || tc.column_name || ' ' || tc.type ||
      CASE WHEN tc.not_null = 'NOT NULL' THEN ' NOT NULL' ELSE '' END ||
      CASE WHEN tc.default_value IS NOT NULL THEN ' DEFAULT ' || tc.default_value ELSE '' END
      ORDER BY tc.attnum
    ),
    E',\\n'
  ) || E'\\n);' ||
  CASE WHEN t.table_comment IS NOT NULL THEN
    E'\\n\\nCOMMENT ON TABLE ' || tc.schemaname || '.' || tc.tablename ||
    ' IS ' || quote_literal(t.table_comment) || ';'
  ELSE '' END ||
  CASE WHEN COUNT(tc.column_comment) FILTER (WHERE tc.column_comment IS NOT NULL) > 0 THEN
    E'\\n\\n' || string_agg(
      CASE WHEN tc.column_comment IS NOT NULL THEN
        'COMMENT ON COLUMN ' || tc.schemaname || '.' || tc.tablename || '.' || tc.column_name ||
        ' IS ' || quote_literal(tc.column_comment) || ';'
      ELSE NULL END,
      E'\\n'
    )
  ELSE '' END AS create_statement
FROM table_columns tc
LEFT JOIN table_comments t ON tc.schemaname = t.schemaname AND tc.tablename = t.tablename
`.trim();

const TABLE_SCHEMAS_SELECT = `
SELECT
  tc.schemaname,
  tc.tablename,
  'CREATE TABLE ' || tc.schemaname || '.' || tc.tablename || E'(\\n' ||
  array_to_string(
    array_agg(
      '    ' || tc.column_name || ' ' || tc.type ||
      CASE WHEN tc.not_null = 'NOT NULL' THEN ' NOT NULL' ELSE '' END ||
      CASE WHEN tc.default_value IS NOT NULL THEN ' DEFAULT ' || tc.default_value ELSE '' END
      ORDER BY tc.attnum
    ),
    E',\\n'
  ) || E'\\n);' ||
  CASE WHEN t.table_comment IS NOT NULL THEN
    E'\\n\\nCOMMENT ON TABLE ' || tc.schemaname || '.' || tc.tablename ||
    ' IS ' || quote_literal(t.table_comment) || ';'
  ELSE '' END ||
  CASE WHEN COUNT(tc.column_comment) FILTER (WHERE tc.column_comment IS NOT NULL) > 0 THEN
    E'\\n\\n' || string_agg(
      CASE WHEN tc.column_comment IS NOT NULL THEN
        'COMMENT ON COLUMN ' || tc.schemaname || '.' || tc.tablename || '.' || tc.column_name ||
        ' IS ' || quote_literal(tc.column_comment) || ';'
      ELSE NULL END,
      E'\\n'
    )
  ELSE '' END AS create_statement
FROM table_columns tc
LEFT JOIN table_comments t ON tc.schemaname = t.schemaname AND tc.tablename = t.tablename
`.trim();

const TABLE_SCHEMAS_GROUP_ORDER = `
GROUP BY tc.schemaname, tc.tablename, t.table_comment
ORDER BY tc.schemaname, tc.tablename
`.trim();

function buildViewSchemasQuery(args: ViewSchemasArgs) {
  const conditions: string[] = [];
  const params: QueryParameter[] = [];
  let paramIndex = 1;

  if (args.schema) {
    conditions.push(`pv.schemaname = $${paramIndex++}`);
    params.push(args.schema);
  }

  if (args.view) {
    conditions.push(`pv.viewname = $${paramIndex++}`);
    params.push(args.view);
  }

  let query = VIEW_SCHEMAS_BASE;
  if (conditions.length > 0) {
    query += `\nAND ${conditions.join(' AND ')}`;
  }
  query += '\nORDER BY pv.schemaname, pv.viewname';

  return { query, params };
}

function buildTableSchemasQuery(args: TableSchemasArgs) {
  const conditions: string[] = [];
  const params: QueryParameter[] = [];
  let paramIndex = 1;

  if (args.schema) {
    conditions.push(`tc.schemaname = $${paramIndex++}`);
    params.push(args.schema);
  }

  if (args.table) {
    conditions.push(`tc.tablename = $${paramIndex++}`);
    params.push(args.table);
  }

  let query = TABLE_SCHEMAS_BASE_CTE;
  if (conditions.length > 0) {
    query += `\nWHERE ${conditions.join(' AND ')}`;
  }
  query += `\n${TABLE_SCHEMAS_GROUP_ORDER}`;

  return { query, params };
}

type QueryArgs = z.infer<typeof QueryInputSchema>;
type ViewSchemasArgs = z.infer<typeof ViewSchemasInputSchema>;
type TableSchemasArgs = z.infer<typeof TableSchemasInputSchema>;
type QueryParameter = string | number | boolean | null;
type QueryResult = z.infer<typeof QueryOutputSchema>;

class PostgresServer {
  private static readonly CONNECTION_ERROR_CODES = new Set([
    '57P01', // admin_shutdown
    '57P02', // crash_shutdown
    '57P03', // cannot_connect_now
    '57P04', // database_dropped
    '08000', // connection_exception
    '08001', // sql client unable to establish
    '08003', // connection_does_not_exist
    '08006', // connection_failure
    '08007', // transaction_resolution_unknown
    '08P01', // protocol_violation
    'ECONNRESET',
    'ECONNREFUSED',
    'ENOTFOUND',
    'EPIPE',
    'PROTOCOL_CONNECTION_LOST',
  ]);

  private static readonly CONNECTION_ERROR_PATTERNS = [
    'terminating connection due to administrator command',
    'server closed the connection unexpectedly',
    'connection terminated unexpectedly',
    'connection closed',
    'has encountered a connection error and is not queryable',
    'client was closed and is not queryable',
    'socket hang up',
    'broken pipe',
  ];

  private readonly mcp: McpServer;
  private readonly connections = new Map<string, Client>();
  private readonly connectionPromises = new Map<string, Promise<Client>>();
  private readonly clientListeners = new Map<
    Client,
    { onEnd: () => void; onError: (error: Error) => void }
  >();
  private readonly readOnlyMode: boolean;
  private readonly queryValidator: QueryValidator;
  private readonly serverName: string;
  private readonly toolName: string;
  private transport?: StdioServerTransport;

  constructor() {
    this.serverName = process.env.MCP_SERVER_NAME ?? 'postgres-mcp';
    this.toolName = `${this.serverName}_query`;

    this.readOnlyMode = process.env.READ_ONLY_MODE !== 'false';
    this.queryValidator = new QueryValidator(this.readOnlyMode);

    this.mcp = new McpServer(
      {
        name: this.serverName,
        version: '1.0.0',
      },
      {
        capabilities: {
          logging: {},
        },
      }
    );

    this.mcp.server.registerCapabilities({
      logging: {},
      tools: { listChanged: true },
    });

    this.mcp.server.onclose = () => {
      void this.handleTransportClosed('server');
    };

    this.registerTools();
  }

  private registerTools() {
    this.mcp.registerTool(
      this.toolName,
      {
        title: `${this.serverName} Query`,
        description: 'Execute a PostgreSQL query',
        inputSchema: QueryInputShape,
        outputSchema: QueryOutputShape,
        annotations: { readOnlyHint: this.readOnlyMode },
      },
      async (args) => this.executeQuery(args)
    );

    this.registerPredefinedQueryTool('view_schemas', {
      title: 'View Schemas',
      description: 'List non-system PostgreSQL view definitions by schema',
      inputSchema: ViewSchemasInputSchema,
      buildQuery: buildViewSchemasQuery,
      defaultMaxRows: 1_000_000,
    });

    this.registerPredefinedQueryTool('table_schemas', {
      title: 'Table Schemas',
      description: 'View table schemas (with comments) - generates CREATE TABLE statements',
      inputSchema: TableSchemasInputSchema,
      buildQuery: buildTableSchemasQuery,
      defaultMaxRows: 1_000_000,
    });
  }

  private async log(level: LoggingLevel, data: unknown) {
    if (!this.mcp.isConnected()) {
      return;
    }

    await this.mcp.sendLoggingMessage({
      level,
      logger: this.serverName,
      data,
    });
  }

  private registerPredefinedQueryTool<T extends z.ZodObject<z.ZodRawShape>>(
    name: string,
    config: {
      title: string;
      description: string;
      inputSchema: T;
      buildQuery: (args: z.infer<T>) => {
        query: string;
        params?: QueryParameter[];
      };
      defaultMaxRows?: number;
    }
  ) {
    const toolName = `${this.serverName}_${name}`;

    this.mcp.registerTool(
      toolName,
      {
        title: config.title,
        description: config.description,
        inputSchema: config.inputSchema.shape,
        outputSchema: QueryOutputShape,
        annotations: { readOnlyHint: this.readOnlyMode },
      },
      async (args: z.infer<T>) => {
        const { query, params } = config.buildQuery(args);
        return this.executeQuery(
          {
            query,
            maxRows: args.maxRows ?? config.defaultMaxRows,
            timeoutMs: args.timeoutMs,
          },
          params
        );
      }
    );
  }

  private describeConnection(connectionString: string): string {
    try {
      const url = new URL(connectionString);
      const user = url.username ? `${decodeURIComponent(url.username)}@` : '';
      const host = url.hostname || 'localhost';
      const port = url.port ? `:${url.port}` : '';
      const database = url.pathname ? url.pathname.replace(/^\//, '') : '';
      const databaseSegment = database ? `/${database}` : '';
      return `${url.protocol}//${user}${host}${port}${databaseSegment}`;
    } catch {
      return '[unparseable connection string]';
    }
  }

  private async ensureClient(connectionString: string): Promise<Client> {
    const existing = this.connections.get(connectionString);
    if (existing) {
      return existing;
    }

    const pending = this.connectionPromises.get(connectionString);
    if (pending) {
      return pending;
    }

    const clientPromise = this.createClient(connectionString);
    this.connectionPromises.set(connectionString, clientPromise);

    try {
      const client = await clientPromise;
      this.connections.set(connectionString, client);
      return client;
    } finally {
      this.connectionPromises.delete(connectionString);
    }
  }

  private async createClient(connectionString: string): Promise<Client> {
    const client = new Client({ connectionString });

    try {
      await client.connect();
      this.registerClientLifecycle(connectionString, client);

      if (this.readOnlyMode) {
        await client.query('SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY');
        await this.log('info', {
          message: 'Connection established in read-only mode',
          connection: this.describeConnection(connectionString),
          readOnlyMode: true,
        });
      } else {
        await this.log('info', {
          message: 'Connection established',
          connection: this.describeConnection(connectionString),
          readOnlyMode: false,
        });
      }

      return client;
    } catch (error) {
      await this.safeCloseClient(client);
      throw new McpError(
        ErrorCode.InternalError,
        `Failed to connect to database: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }

  private registerClientLifecycle(connectionString: string, client: Client) {
    const cleanup = () => {
      client.off('end', onEnd);
      client.off('error', onError);
      this.clientListeners.delete(client);
    };

    const onEnd = () => {
      if (this.connections.get(connectionString) === client) {
        this.connections.delete(connectionString);
      }
      cleanup();
    };

    const onError = (error: Error) => {
      if (this.connections.get(connectionString) === client) {
        this.connections.delete(connectionString);
      }
      cleanup();
      process.stderr.write(
        `[${this.serverName}] connection error (${this.describeConnection(connectionString)}): ${
          error.message
        }\n`
      );
    };

    client.on('end', onEnd);
    client.on('error', onError);

    this.clientListeners.set(client, { onEnd, onError });
  }

  private async safeCloseClient(client: Client) {
    try {
      await client.end();
    } catch (error) {
      process.stderr.write(
        `[${this.serverName}] failed to close database client: ${
          error instanceof Error ? error.message : String(error)
        }\n`
      );
    }
  }

  private async dropClient(connectionString: string, client: Client, reason?: string) {
    const listeners = this.clientListeners.get(client);
    if (listeners) {
      client.off('end', listeners.onEnd);
      client.off('error', listeners.onError);
      this.clientListeners.delete(client);
    }

    if (this.connections.get(connectionString) === client) {
      this.connections.delete(connectionString);
    }

    if (reason) {
      process.stderr.write(
        `[${this.serverName}] dropping connection (${this.describeConnection(connectionString)}): ${reason}\n`
      );
    }

    await this.safeCloseClient(client);
  }

  private async disposeAllClients() {
    const pending = Array.from(this.connectionPromises.values());
    this.connectionPromises.clear();
    await Promise.allSettled(pending);

    const entries = Array.from(this.connections.entries());
    this.connections.clear();

    await Promise.all(
      entries.map(([connectionString, client]) => this.dropClient(connectionString, client))
    );
  }

  private buildQuery(args: QueryArgs) {
    const sanitizedQuery = args.query.trim().replace(/;+\s*$/u, '');
    const effectiveMaxRows = args.maxRows ?? 10;

    const shouldLimit =
      typeof effectiveMaxRows === 'number' &&
      Number.isInteger(effectiveMaxRows) &&
      effectiveMaxRows > 0 &&
      /^select/i.test(sanitizedQuery);

    if (shouldLimit) {
      return `SELECT * FROM (${sanitizedQuery}) AS __mcp_query LIMIT ${effectiveMaxRows}`;
    }

    return sanitizedQuery;
  }

  private isConnectionError(error: unknown): error is Error & { code?: string } {
    if (!(error instanceof Error)) {
      return false;
    }

    const code = (error as NodeJS.ErrnoException).code;
    if (typeof code === 'string' && PostgresServer.CONNECTION_ERROR_CODES.has(code)) {
      return true;
    }

    const message = error.message.toLowerCase();
    return PostgresServer.CONNECTION_ERROR_PATTERNS.some((pattern) => message.includes(pattern));
  }

  private async executeQuery(args: QueryArgs, queryParameters?: QueryParameter[]) {
    const connectionString = process.env.DATABASE_URL;

    if (!connectionString) {
      throw new McpError(
        ErrorCode.InvalidParams,
        'Connection string is required. Set DATABASE_URL environment variable'
      );
    }

    this.queryValidator.validateOrThrow(args.query);

    const text = this.buildQuery(args);

    let lastError: unknown;

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const client = await this.ensureClient(connectionString);
      let timeoutConfigured = false;

      try {
        if (args.timeoutMs) {
          await client.query('SET statement_timeout = $1', [args.timeoutMs]);
          timeoutConfigured = true;
        }

        const parameterValues = queryParameters ?? [];
        const result =
          parameterValues.length > 0
            ? await client.query({ text, values: parameterValues })
            : await client.query(text);
        const rowCount = typeof result.rowCount === 'number' ? result.rowCount : result.rows.length;

        const structuredContent: QueryResult = {
          rowCount,
          rows: result.rows,
          fields: result.fields?.map((field) => ({
            name: field.name,
            dataTypeID: field.dataTypeID,
          })),
        };

        return {
          structuredContent,
          content: [
            {
              type: 'text',
              text: JSON.stringify(structuredContent, null, 2),
            } satisfies TextContent,
          ],
        };
      } catch (error) {
        lastError = error;

        if (this.isConnectionError(error)) {
          await this.dropClient(connectionString, client, 'connection error');

          await this.log('warning', {
            message: 'Database connection reset; retrying query',
            connection: this.describeConnection(connectionString),
            attempt,
          });

          if (attempt === 0) {
            continue;
          }
        }

        return {
          isError: true,
          content: [
            {
              type: 'text',
              text: error instanceof Error ? error.message : 'Unknown error',
            } satisfies TextContent,
          ],
        };
      } finally {
        if (timeoutConfigured && this.connections.get(connectionString) === client) {
          try {
            await client.query('RESET statement_timeout');
          } catch (resetError) {
            await this.log('warning', {
              message: 'Failed to reset statement_timeout after query',
              error: resetError instanceof Error ? resetError.message : String(resetError),
            });
          }
        }
      }
    }

    return {
      isError: true,
      content: [
        {
          type: 'text',
          text: lastError instanceof Error ? lastError.message : 'Unknown error',
        } satisfies TextContent,
      ],
    };
  }

  async run() {
    if (this.transport) {
      throw new Error('Server is already running');
    }

    this.transport = new StdioServerTransport();
    this.transport.onclose = () => {
      void this.handleTransportClosed('transport');
    };

    try {
      await this.mcp.connect(this.transport);
      await this.log('info', { message: `${this.serverName} server running on stdio` });
    } catch (error) {
      if (this.mcp.isConnected()) {
        await this.log('error', {
          message: 'Server failed to start',
          error: error instanceof Error ? error.message : String(error),
        });
      }
      throw error;
    }
  }

  private async handleTransportClosed(source: string) {
    process.stderr.write(
      `[${this.serverName}] MCP transport closed (${source}), clearing cached database connections.\n`
    );
    await this.disposeAllClients();
    this.transport = undefined;
  }

  async shutdown() {
    try {
      if (this.mcp.isConnected()) {
        await this.mcp.close();
      }
    } catch (error) {
      process.stderr.write(
        `Failed to close MCP server: ${error instanceof Error ? error.message : String(error)}\n`
      );
    }

    if (this.transport) {
      try {
        await this.transport.close?.();
      } catch (closeError) {
        process.stderr.write(
          `Failed to close transport: ${closeError instanceof Error ? closeError.message : String(closeError)}\n`
        );
      } finally {
        this.transport = undefined;
      }
    }

    await this.disposeAllClients();
  }
}

const server = new PostgresServer();

// Graceful termination handlers
const gracefulExit = async (signal: string) => {
  process.stderr.write(`\nReceived ${signal}, shutting down gracefully...\n`);
  try {
    await server.shutdown();
    process.exit(0);
  } catch (error) {
    process.stderr.write(
      `Failed to shut down cleanly: ${error instanceof Error ? error.message : String(error)}\n`
    );
    process.exit(1);
  }
};

process.on('SIGINT', () => void gracefulExit('SIGINT'));
process.on('SIGTERM', () => void gracefulExit('SIGTERM'));

// Start the server
server.run().catch(async (error) => {
  process.stderr.write(
    `Server failed to start: ${error instanceof Error ? error.message : String(error)}\n`
  );
  process.exit(1);
});
