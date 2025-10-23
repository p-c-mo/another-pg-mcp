#!/usr/bin/env node
import dotenv from 'dotenv';
import cors from 'cors';
import express, { type Request, type Response } from 'express';
import { randomUUID } from 'node:crypto';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import type { Transport } from '@modelcontextprotocol/sdk/shared/transport.js';
import { ErrorCode, McpError, isInitializeRequest } from '@modelcontextprotocol/sdk/types.js';
import type { LoggingLevel, TextContent } from '@modelcontextprotocol/sdk/types.js';
import { Client } from 'pg';
import { z } from 'zod';
import { QueryValidator } from './queryValidator.js';

dotenv.config();

const SERVER_NAME = process.env.MCP_SERVER_NAME ?? 'postgres-mcp';

const DATABASE_URL = (() => {
  const value = process.env.DATABASE_URL;
  if (!value) {
    console.error(
      `[${SERVER_NAME}] DATABASE_URL environment variable is required. Define it in your environment or .env file before starting the server.`
    );
    process.exit(1);
  }
  return value;
})();

const resolveReadOnlyMode = (): boolean => {
  const raw = process.env.MCP_READ_ONLY_MODE ?? process.env.READ_ONLY_MODE;
  if (typeof raw !== 'string') {
    return true;
  }

  const normalized = raw.trim().toLowerCase();
  if (normalized.length === 0) {
    return true;
  }

  return normalized !== 'false';
};

const QueryInputShape = {
  query: z.string().describe('SQL to execute'),
  params: z
    .array(z.union([z.string(), z.number(), z.boolean(), z.null()]))
    .optional()
    .describe('Positional parameters ($1, $2, ...)'),
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

const QueryOutputShape = {
  rowCount: z.number(),
  rows: z.array(z.record(z.any())),
  fields: z
    .array(z.object({ name: z.string(), dataTypeID: z.number() }))
    .optional(),
};

const QueryOutputSchema = z.object(QueryOutputShape);

const PredefinedQueryInputShape = {
  maxRows: QueryInputShape.maxRows,
  timeoutMs: QueryInputShape.timeoutMs,
  schema: z
    .string()
    .optional()
    .describe('Limit results to a specific schema'),
  table: z
    .string()
    .optional()
    .describe('Limit results to a specific table name'),
  view: z
    .string()
    .optional()
    .describe('Limit results to a specific view name'),
};

const PredefinedQueryInputSchema = z.object(PredefinedQueryInputShape);

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
  'CREATE TABLE ' || tc.schemaname || '.' || tc.tablename || E'(\n' ||
  array_to_string(
    array_agg(
      '    ' || tc.column_name || ' ' || tc.type ||
      CASE WHEN tc.not_null = 'NOT NULL' THEN ' NOT NULL' ELSE '' END ||
      CASE WHEN tc.default_value IS NOT NULL THEN ' DEFAULT ' || tc.default_value ELSE '' END
      ORDER BY tc.attnum
    ),
    E',\n'
  ) || E'\n);' ||
  CASE WHEN t.table_comment IS NOT NULL THEN
    E'\n\nCOMMENT ON TABLE ' || tc.schemaname || '.' || tc.tablename ||
    ' IS ' || quote_literal(t.table_comment) || ';'
  ELSE '' END ||
  CASE WHEN COUNT(tc.column_comment) FILTER (WHERE tc.column_comment IS NOT NULL) > 0 THEN
    E'\n\n' || string_agg(
      CASE WHEN tc.column_comment IS NOT NULL THEN
        'COMMENT ON COLUMN ' || tc.schemaname || '.' || tc.tablename || '.' || tc.column_name ||
        ' IS ' || quote_literal(tc.column_comment) || ';'
      ELSE NULL END,
      E'\n'
    )
  ELSE '' END AS create_statement
FROM table_columns tc
LEFT JOIN table_comments t ON tc.schemaname = t.schemaname AND tc.tablename = t.tablename
`.trim();

const TABLE_SCHEMAS_SELECT = `
SELECT
  tc.schemaname,
  tc.tablename,
  'CREATE TABLE ' || tc.schemaname || '.' || tc.tablename || E'(\n' ||
  array_to_string(
    array_agg(
      '    ' || tc.column_name || ' ' || tc.type ||
      CASE WHEN tc.not_null = 'NOT NULL' THEN ' NOT NULL' ELSE '' END ||
      CASE WHEN tc.default_value IS NOT NULL THEN ' DEFAULT ' || tc.default_value ELSE '' END
      ORDER BY tc.attnum
    ),
    E',\n'
  ) || E'\n);' ||
  CASE WHEN t.table_comment IS NOT NULL THEN
    E'\n\nCOMMENT ON TABLE ' || tc.schemaname || '.' || tc.tablename ||
    ' IS ' || quote_literal(t.table_comment) || ';'
  ELSE '' END ||
  CASE WHEN COUNT(tc.column_comment) FILTER (WHERE tc.column_comment IS NOT NULL) > 0 THEN
    E'\n\n' || string_agg(
      CASE WHEN tc.column_comment IS NOT NULL THEN
        'COMMENT ON COLUMN ' || tc.schemaname || '.' || tc.tablename || '.' || tc.column_name ||
        ' IS ' || quote_literal(tc.column_comment) || ';'
      ELSE NULL END,
      E'\n'
    )
  ELSE '' END AS create_statement
FROM table_columns tc
LEFT JOIN table_comments t ON tc.schemaname = t.schemaname AND tc.tablename = t.tablename
`.trim();

const TABLE_SCHEMAS_GROUP_ORDER = `
GROUP BY tc.schemaname, tc.tablename, t.table_comment
ORDER BY tc.schemaname, tc.tablename
`.trim();

function buildViewSchemasQuery(args: PredefinedQueryArgs) {
  const conditions: string[] = [];
  const params: (string | number)[] = [];
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

function buildTableSchemasQuery(args: PredefinedQueryArgs) {
  const conditions: string[] = [];
  const params: (string | number)[] = [];
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
type PredefinedQueryArgs = z.infer<typeof PredefinedQueryInputSchema>;
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
  private readonly databaseUrl: string;
  private readonly toolName: string;
  private transport?: Transport;
  private sessionId?: string;

  constructor() {
    this.serverName = SERVER_NAME;
    this.databaseUrl = DATABASE_URL;
    this.toolName = `${this.serverName}_query`;

    this.readOnlyMode = resolveReadOnlyMode();
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
    this.consoleLog('info', 'PostgresServer initialized', {
      readOnlyMode: this.readOnlyMode,
    });
  }

  setSessionId(sessionId: string) {
    this.sessionId = sessionId;
    this.consoleLog('info', 'Session identifier attached');
  }

  getSessionId(): string | undefined {
    return this.sessionId;
  }

  private consoleLog(
    level: 'debug' | 'info' | 'warn' | 'error',
    message: string,
    context?: Record<string, unknown>
  ) {
    const prefixParts = [`[${this.serverName}]`];
    if (this.sessionId) {
      prefixParts.push(`[session:${this.sessionId}]`);
    }
    const baseMessage = `${prefixParts.join(' ')} ${message}`;
    const contextSuffix =
      context && Object.keys(context).length > 0 ? ` ${JSON.stringify(context)}` : '';
    const finalMessage = `${baseMessage}${contextSuffix}`;

    switch (level) {
      case 'error':
        console.error(finalMessage);
        break;
      case 'warn':
        console.warn(finalMessage);
        break;
      case 'debug':
        console.debug(finalMessage);
        break;
      default:
        console.log(finalMessage);
    }
  }

  async connect(
    transport: Transport,
    options: {
      label?: string;
      onTransportClosed?: () => void;
      onTransportError?: (error: Error) => void;
    } = {}
  ) {
    if (this.transport) {
      throw new Error('Server is already connected to a transport');
    }

    const transportLabel = options.label ?? 'transport';
    this.consoleLog('info', 'Attaching transport', { transport: transportLabel });

    this.transport = transport;
    transport.onclose = () => {
      options.onTransportClosed?.();
      void this.handleTransportClosed(transportLabel);
    };
    transport.onerror = (error: Error) => {
      this.consoleLog('error', 'Transport error observed', {
        transport: transportLabel,
        error: error.message,
      });
      options.onTransportError?.(error);
    };

    try {
      await this.mcp.connect(transport);
      await this.log('info', { message: `${this.serverName} server running on ${transportLabel}` });
      this.consoleLog('info', 'MCP server connected', { transport: transportLabel });
    } catch (error) {
      this.transport = undefined;
      const formattedError = error instanceof Error ? error.message : String(error);
      this.consoleLog('error', 'Failed to connect transport', {
        transport: transportLabel,
        error: formattedError,
      });

      if (this.mcp.isConnected()) {
        await this.log('error', {
          message: 'Server failed to start',
          error: formattedError,
        });
      }

      throw error;
    }
  }

  private registerTools() {
    this.mcp.registerTool(
      this.toolName,
      {
        title: `${this.serverName} Query`,
        description: 'Execute a PostgreSQL query with optional parameters',
        inputSchema: QueryInputShape,
        outputSchema: QueryOutputShape,
        annotations: { readOnlyHint: this.readOnlyMode },
      },
      async (args) => this.executeQuery(args)
    );

    this.registerPredefinedQueryTool('view_schemas', {
      title: 'View Schemas',
      description: 'List non-system PostgreSQL view definitions by schema',
      buildQuery: buildViewSchemasQuery,
      defaultMaxRows: 1_000_000,
    });

    this.registerPredefinedQueryTool('table_schemas', {
      title: 'Table Schemas',
      description: 'View table schemas (with comments) - generates CREATE TABLE statements',
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

  private registerPredefinedQueryTool(
    name: string,
    config: {
      title: string;
      description: string;
      buildQuery: (args: PredefinedQueryArgs) => {
        query: string;
        params?: QueryArgs['params'];
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
        inputSchema: PredefinedQueryInputShape,
        outputSchema: QueryOutputShape,
        annotations: { readOnlyHint: this.readOnlyMode },
      },
      async (args: PredefinedQueryArgs) => {
        const { query, params } = config.buildQuery(args);
        return this.executeQuery({
          query,
          params,
          maxRows: args.maxRows ?? config.defaultMaxRows,
          timeoutMs: args.timeoutMs,
        });
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
      this.consoleLog('debug', 'Reusing cached database connection', {
        connection: this.describeConnection(connectionString),
      });
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
      this.consoleLog('info', 'Opening new database connection', {
        connection: this.describeConnection(connectionString),
      });
      await client.connect();
      this.registerClientLifecycle(connectionString, client);

      if (this.readOnlyMode) {
        await client.query('SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY');
        await this.log('info', {
          message: 'Connection established in read-only mode',
          connection: this.describeConnection(connectionString),
          readOnlyMode: true,
        });
        this.consoleLog('info', 'Connection established in read-only mode', {
          connection: this.describeConnection(connectionString),
        });
      } else {
        await this.log('info', {
          message: 'Connection established',
          connection: this.describeConnection(connectionString),
          readOnlyMode: false,
        });
        this.consoleLog('info', 'Connection established', {
          connection: this.describeConnection(connectionString),
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
      this.consoleLog('info', 'Database connection ended', {
        connection: this.describeConnection(connectionString),
      });
    };

    const onError = (error: Error) => {
      if (this.connections.get(connectionString) === client) {
        this.connections.delete(connectionString);
      }
      cleanup();
      this.consoleLog('error', 'Database connection error detected', {
        connection: this.describeConnection(connectionString),
        error: error.message,
      });
    };

    client.on('end', onEnd);
    client.on('error', onError);

    this.clientListeners.set(client, { onEnd, onError });
  }

  private async safeCloseClient(client: Client) {
    try {
      await client.end();
    } catch (error) {
      this.consoleLog('warn', 'Failed to close database client', {
        error: error instanceof Error ? error.message : String(error),
      });
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
      this.consoleLog('warn', 'Dropping cached database connection', {
        connection: this.describeConnection(connectionString),
        reason,
      });
    }

    await this.safeCloseClient(client);
  }

  private async disposeAllClients() {
    const pending = Array.from(this.connectionPromises.values());
    this.connectionPromises.clear();
    await Promise.allSettled(pending);

    const entries = Array.from(this.connections.entries());
    this.connections.clear();

    if (entries.length > 0) {
      this.consoleLog('info', 'Disposing remaining database clients', {
        count: entries.length,
      });
    }

    await Promise.all(
      entries.map(([connectionString, client]) => this.dropClient(connectionString, client))
    );
  }

  private buildQuery(args: QueryArgs) {
    const sanitizedQuery = args.query.trim().replace(/;+\s*$/u, '');
    const values = [...(args.params ?? [])];
    const effectiveMaxRows = args.maxRows ?? 10;

    const shouldLimit =
      typeof effectiveMaxRows === 'number' &&
      effectiveMaxRows > 0 &&
      /^select/i.test(sanitizedQuery);

    if (shouldLimit) {
      const limitParamPosition = values.length + 1;
      return {
        text: `SELECT * FROM (${sanitizedQuery}) AS __mcp_query LIMIT $${limitParamPosition}`,
        values: [...values, effectiveMaxRows],
      };
    }

    return {
      text: sanitizedQuery,
      values,
    };
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

  private async executeQuery(args: QueryArgs) {
    const connectionString = this.databaseUrl;

    this.queryValidator.validateOrThrow(args.query);

    const { text, values } = this.buildQuery(args);
    const connectionDescription = this.describeConnection(connectionString);
    const querySummary = text.length > 500 ? `${text.slice(0, 500)}…` : text;
    this.consoleLog('info', 'Executing query', {
      connection: connectionDescription,
      hasParams: values.length > 0,
      maxRows: args.maxRows ?? 10,
    });

    let lastError: unknown;

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const client = await this.ensureClient(connectionString);
      let timeoutConfigured = false;
      const start = Date.now();

      try {
        if (args.timeoutMs) {
          await client.query('SET statement_timeout = $1', [args.timeoutMs]);
          timeoutConfigured = true;
        }

        const result = await client.query({ text, values });
        const rowCount = typeof result.rowCount === 'number' ? result.rowCount : result.rows.length;
        const durationMs = Date.now() - start;

        const structuredContent: QueryResult = {
          rowCount,
          rows: result.rows,
          fields: result.fields?.map((field) => ({
            name: field.name,
            dataTypeID: field.dataTypeID,
          })),
        };

        this.consoleLog('info', 'Query succeeded', {
          connection: connectionDescription,
          rowCount,
          durationMs,
        });

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

        if (error instanceof McpError) {
          this.consoleLog('warn', 'Query failed with MCP error', {
            connection: connectionDescription,
            error: error.message,
            attempt,
          });
          throw error;
        }

        if (error instanceof Error) {
          await this.log('error', {
            message: 'Database query failed',
            error: error.message,
            connection: this.describeConnection(connectionString),
            attempt,
          });
        }

        if (error instanceof Error && this.isConnectionError(error)) {
          const connection = this.connections.get(connectionString);

          if (connection) {
            await this.dropClient(connectionString, connection, error.message);
          }

          await this.log('warning', {
            message: 'Database connection error, retrying query',
            error: error.message,
            connection: this.describeConnection(connectionString),
            attempt,
          });
          this.consoleLog('warn', 'Database connection error during query execution', {
            connection: connectionDescription,
            error: error.message,
            attempt,
          });

          if (attempt === 0) {
            continue;
          }
        }

        this.consoleLog('error', 'Query failed', {
          connection: connectionDescription,
          error: error instanceof Error ? error.message : 'Unknown error',
          attempt,
          query: querySummary,
        });

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
            this.consoleLog('warn', 'Failed to reset statement_timeout', {
              connection: connectionDescription,
              error: resetError instanceof Error ? resetError.message : String(resetError),
            });
          }
        }
      }
    }

    this.consoleLog('error', 'Query failed after retries', {
      connection: connectionDescription,
      error: lastError instanceof Error ? lastError.message : 'Unknown error',
      query: querySummary,
    });

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

  private async handleTransportClosed(source: string) {
    this.consoleLog('info', 'Transport closed, clearing cached database connections', {
      source,
    });
    await this.disposeAllClients();
    this.transport = undefined;
  }

  async shutdown() {
    this.consoleLog('info', 'Shutting down PostgresServer instance');
    try {
      if (this.mcp.isConnected()) {
        await this.mcp.close();
      }
    } catch (error) {
      this.consoleLog('error', 'Failed to close MCP server', {
        error: error instanceof Error ? error.message : String(error),
      });
    }

    if (this.transport) {
      try {
        await this.transport.close?.();
      } catch (closeError) {
        this.consoleLog('error', 'Failed to close transport', {
          error: closeError instanceof Error ? closeError.message : String(closeError),
        });
      } finally {
        this.transport = undefined;
      }
    }

    await this.disposeAllClients();
    this.consoleLog('info', 'PostgresServer shutdown complete');
  }
}

type SessionEntry = {
  transport: StreamableHTTPServerTransport;
  server: PostgresServer;
};

type LogLevel = 'debug' | 'info' | 'warn' | 'error';

const serverName = SERVER_NAME;

const globalLog = (level: LogLevel, message: string, context?: Record<string, unknown>) => {
  const prefix = `[${serverName}] [http]`;
  const contextSuffix =
    context && Object.keys(context).length > 0 ? ` ${JSON.stringify(context)}` : '';
  const finalMessage = `${prefix} ${message}${contextSuffix}`;

  switch (level) {
    case 'error':
      console.error(finalMessage);
      break;
    case 'warn':
      console.warn(finalMessage);
      break;
    case 'debug':
      console.debug(finalMessage);
      break;
    default:
      console.log(finalMessage);
  }
};

const normalizePort = (value: string | undefined, fallback: number): number => {
  if (!value) {
    return fallback;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isNaN(parsed) ? fallback : parsed;
};

const extractJsonRpcId = (body: unknown): string | number | null | undefined => {
  if (!body || typeof body !== 'object') {
    return undefined;
  }

  if ('id' in body) {
    const id = (body as { id?: unknown }).id;
    if (typeof id === 'string' || typeof id === 'number' || id === null) {
      return id;
    }
  }

  return undefined;
};

const previewBody = (body: unknown): string => {
  try {
    const serialized = typeof body === 'string' ? body : JSON.stringify(body);
    return serialized ? serialized.slice(0, 200) : '';
  } catch {
    return '[unserializable body]';
  }
};

const MCP_PORT = normalizePort(process.env.MCP_PORT, 6123);

const deriveAllowedHosts = (): string[] => {
  const hosts = new Set<string>(['127.0.0.1', 'localhost']);

  if (Number.isFinite(MCP_PORT)) {
    hosts.add(`127.0.0.1:${MCP_PORT}`);
    hosts.add(`localhost:${MCP_PORT}`);
  }

  const envHosts = process.env.MCP_ALLOWED_HOSTS;
  if (envHosts) {
    envHosts
      .split(',')
      .map((host) => host.trim())
      .filter((host) => host.length > 0)
      .forEach((host) => hosts.add(host));
  }

  return Array.from(hosts);
};

const derivedAllowedHosts = deriveAllowedHosts();

globalLog('info', 'Starting Streamable HTTP MCP server', {
  port: MCP_PORT,
  readOnlyMode: resolveReadOnlyMode(),
});

const app = express();

app.use(
  cors({
    origin: true,
    credentials: true,
    exposedHeaders: ['Mcp-Session-Id'],
  })
);
app.use(express.json({ limit: '5mb' }));

const sessions = new Map<string, SessionEntry>();

const createSessionServer = () => new PostgresServer();

const getSessionEntry = (sessionId: string | undefined) => {
  if (!sessionId) {
    return undefined;
  }
  return sessions.get(sessionId);
};

app.post('/mcp', async (req: Request, res: Response) => {
  const sessionIdHeader = req.header('mcp-session-id') ?? undefined;
  const sessionEntry = getSessionEntry(sessionIdHeader);
  const requestId = extractJsonRpcId(req.body);

  globalLog('info', 'Received POST /mcp', {
    sessionId: sessionIdHeader ?? 'new',
    hasSession: Boolean(sessionEntry),
    requestId: requestId ?? null,
    bodyPreview: previewBody(req.body),
  });

  let createdTransport: StreamableHTTPServerTransport | undefined;
  let createdServer: PostgresServer | undefined;

  try {
    if (sessionEntry) {
      await sessionEntry.transport.handleRequest(req, res, req.body);
      return;
    }

    if (!isInitializeRequest(req.body)) {
      res.status(400).json({
        jsonrpc: '2.0',
        error: { code: -32000, message: 'Bad Request: No valid session ID provided' },
        id: null,
      });
      return;
    }

    createdServer = createSessionServer();
    createdTransport = new StreamableHTTPServerTransport({
      sessionIdGenerator: () => randomUUID(),
      enableDnsRebindingProtection: true,
      allowedHosts: derivedAllowedHosts,
      onsessioninitialized: (initializedSessionId) => {
        createdServer?.setSessionId(initializedSessionId);
        sessions.set(initializedSessionId, { transport: createdTransport!, server: createdServer! });
        globalLog('info', 'Session initialized', { sessionId: initializedSessionId });
      },
    });

    await createdServer.connect(createdTransport, {
      label: 'streamable-http',
      onTransportClosed: () => {
        const activeSessionId = createdTransport?.sessionId ?? createdServer?.getSessionId();
        if (activeSessionId && sessions.has(activeSessionId)) {
          sessions.delete(activeSessionId);
          globalLog('info', 'Transport closed and session removed', {
            sessionId: activeSessionId,
          });
        }
      },
      onTransportError: (error) => {
        globalLog('error', 'Transport error emitted', {
          sessionId: createdTransport?.sessionId ?? createdServer?.getSessionId(),
          error: error.message,
        });
      },
    });

    await createdTransport.handleRequest(req, res, req.body);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    globalLog('error', 'Error handling POST /mcp', {
      sessionId: sessionIdHeader ?? createdTransport?.sessionId ?? 'unknown',
      error: message,
    });

    if (!res.headersSent) {
      res.status(500).json({
        jsonrpc: '2.0',
        error: { code: -32603, message: 'Internal server error' },
        id: null,
      });
    }

    if (createdServer && (!createdTransport?.sessionId || !sessions.has(createdTransport.sessionId))) {
      try {
        await createdServer.shutdown();
      } catch (shutdownError) {
        globalLog('error', 'Failed to shut down server after POST error', {
          error: shutdownError instanceof Error ? shutdownError.message : String(shutdownError),
        });
      }
    }
  }
});

app.get('/mcp', async (req: Request, res: Response) => {
  const sessionId = req.header('mcp-session-id') ?? undefined;
  const entry = getSessionEntry(sessionId);

  globalLog('info', 'Received GET /mcp', {
    sessionId: sessionId ?? 'missing',
    hasSession: Boolean(entry),
    lastEventId: req.header('last-event-id') ?? null,
  });

  if (!entry) {
    res.status(400).send('Invalid or missing session ID');
    return;
  }

  try {
    await entry.transport.handleRequest(req, res);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    globalLog('error', 'Error handling GET /mcp', {
      sessionId,
      error: message,
    });

    if (!res.headersSent) {
      res.status(500).send('Internal server error');
    }
  }
});

app.delete('/mcp', async (req: Request, res: Response) => {
  const sessionId = req.header('mcp-session-id') ?? undefined;
  const entry = getSessionEntry(sessionId);

  globalLog('info', 'Received DELETE /mcp', {
    sessionId: sessionId ?? 'missing',
    hasSession: Boolean(entry),
  });

  if (!entry) {
    res.status(400).send('Invalid or missing session ID');
    return;
  }

  try {
    await entry.transport.handleRequest(req, res);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    globalLog('error', 'Error handling DELETE /mcp', {
      sessionId,
      error: message,
    });

    if (!res.headersSent) {
      res.status(500).send('Internal server error');
    }
  }
});

const httpServer = app.listen(MCP_PORT, () => {
  globalLog('info', 'Streamable HTTP MCP server listening', { port: MCP_PORT });
});

const closeHttpServer = async () =>
  new Promise<void>((resolve) => {
    httpServer.close((error?: Error) => {
      if (error) {
        globalLog('error', 'Error closing HTTP server', { error: error.message });
      } else {
        globalLog('info', 'HTTP server closed');
      }
      resolve();
    });
  });

const shutdownSessions = async () => {
  const sessionIds = Array.from(sessions.keys());
  if (sessionIds.length === 0) {
    return;
  }

  globalLog('info', 'Closing active sessions', { count: sessionIds.length });

  await Promise.all(
    sessionIds.map(async (sessionId) => {
      const entry = sessions.get(sessionId);
      if (!entry) {
        return;
      }

      try {
        globalLog('info', 'Closing session', { sessionId });
        await entry.server.shutdown();
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        globalLog('error', 'Failed to cleanly close session', { sessionId, error: message });
      } finally {
        sessions.delete(sessionId);
      }
    })
  );
};

const gracefulShutdown = async (signal: string) => {
  globalLog('info', 'Received shutdown signal', { signal });

  await shutdownSessions();
  await closeHttpServer();

  globalLog('info', 'Shutdown complete', { signal });
  process.exit(0);
};

process.on('SIGINT', () => {
  void gracefulShutdown('SIGINT');
});

process.on('SIGTERM', () => {
  void gracefulShutdown('SIGTERM');
});

process.on('uncaughtException', (error: Error) => {
  globalLog('error', 'Uncaught exception', { error: error.message, stack: error.stack });
});

process.on('unhandledRejection', (reason: unknown) => {
  globalLog('error', 'Unhandled rejection', { reason: String(reason) });
});
