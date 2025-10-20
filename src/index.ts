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
  connectionString: z
    .string()
    .optional()
    .describe('Overrides DATABASE_URL'),
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

type QueryArgs = z.infer<typeof QueryInputSchema>;
type QueryResult = z.infer<typeof QueryOutputSchema>;

class PostgresServer {
  private readonly mcp: McpServer;
  private connections: Map<string, Client> = new Map();
  private readonly readOnlyMode: boolean;
  private readonly queryValidator: QueryValidator;
  private readonly serverName: string;

  constructor() {
    this.serverName = process.env.MCP_SERVER_NAME ?? 'postgres-mcp';

    // Default to read-only mode unless explicitly disabled
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
    this.mcp.server.registerCapabilities({ logging: {} });

    this.registerTools();
  }

  private registerTools() {
    this.mcp.registerTool(
      'query',
      {
        title: 'PostgreSQL Query',
        description: 'Execute a PostgreSQL query with optional parameters',
        inputSchema: QueryInputShape,
        outputSchema: QueryOutputShape,
        annotations: { readOnlyHint: this.readOnlyMode },
      },
      async (args) => this.executeQuery(args)
    );
  }

  private async log(level: LoggingLevel, data: unknown) {
    if (!this.mcp.isConnected()) {
      return;
    }

    await this.mcp.sendLoggingMessage({
      level,
      logger: 'postgres-mcp',
      data,
    });
  }

  private async ensureClient(connectionString: string): Promise<Client> {
    const connectionKey = connectionString;

    let client = this.connections.get(connectionKey);

    if (!client) {
      client = new Client({ connectionString });
      try {
        await client.connect();

        // Set read-only mode at the session level if enabled
        if (this.readOnlyMode) {
          await client.query('SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY');
          await this.log('info', {
            message: 'Connection established in read-only mode',
            readOnlyMode: this.readOnlyMode,
          });
        }

        this.connections.set(connectionKey, client);
      } catch (error) {
        throw new McpError(
          ErrorCode.InternalError,
          `Failed to connect to database: ${error instanceof Error ? error.message : 'Unknown error'}`
        );
      }
    }

    return client;
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

  private async executeQuery(args: QueryArgs) {
    const connectionString = args.connectionString || process.env.DATABASE_URL;

    if (!connectionString) {
      throw new McpError(
        ErrorCode.InvalidParams,
        'Connection string is required. Provide connectionString parameter or set DATABASE_URL environment variable'
      );
    }

    // Validate the query before execution
    this.queryValidator.validateOrThrow(args.query);

    const client = await this.ensureClient(connectionString);

    const { text, values } = this.buildQuery(args);

    try {
      if (args.timeoutMs) {
        await client.query('SET statement_timeout = $1', [args.timeoutMs]);
      }

      const result = await client.query({ text, values });
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
      if (args.timeoutMs) {
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

  async run() {
    const transport = new StdioServerTransport();
    try {
      await this.mcp.connect(transport);
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
}

const server = new PostgresServer();
server.run().catch(() => {
  process.exit(1);
});
