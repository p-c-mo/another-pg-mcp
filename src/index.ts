#!/usr/bin/env node
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ErrorCode,
  ListToolsRequestSchema,
  McpError,
} from '@modelcontextprotocol/sdk/types.js';
import { Client } from 'pg';
import { QueryValidator } from './queryValidator.js';

interface QueryArgs {
  query: string;
  connectionString?: string;
}

class PostgresServer {
  private server: Server;
  private connections: Map<string, Client> = new Map();
  private readOnlyMode: boolean;
  private queryValidator: QueryValidator;

  constructor() {
    const serverName = process.env.MCP_SERVER_NAME;
    if (!serverName) {
      throw new Error('MCP_SERVER_NAME environment variable is required');
    }
    
    // Default to read-only mode unless explicitly disabled
    this.readOnlyMode = process.env.READ_ONLY_MODE !== 'false';
    this.queryValidator = new QueryValidator(this.readOnlyMode);
    
    this.server = new Server(
      {
        name: serverName,
        version: '1.0.0',
      },
      {
        capabilities: {
          tools: {},
        },
      }
    );

    this.setupHandlers();
  }

  private setupHandlers() {
    const serverName = process.env.MCP_SERVER_NAME!;
    const toolName = `${serverName}_query`;
    
    this.server.setRequestHandler(ListToolsRequestSchema, async () => ({
      tools: [
        {
          name: toolName,
          description: 'Execute a PostgreSQL query',
          inputSchema: {
            type: 'object',
            properties: {
              query: {
                type: 'string',
                description: 'The SQL query to execute',
              },
              connectionString: {
                type: 'string',
                description: 'PostgreSQL connection string (uses DATABASE_URL env var if not provided)',
              },
            },
            required: ['query'],
          },
        },
      ],
    }));

    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      if (request.params.name !== toolName) {
        throw new McpError(
          ErrorCode.MethodNotFound,
          `Unknown tool: ${request.params.name}`
        );
      }

      const args = request.params.arguments as unknown as QueryArgs;
      
      if (!args.query) {
        throw new McpError(
          ErrorCode.InvalidParams,
          'Query parameter is required'
        );
      }

      return await this.executeQuery(args);
    });
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

    const connectionKey = connectionString;
    
    let client = this.connections.get(connectionKey);
    
    if (!client) {
      client = new Client({ connectionString });
      try {
        await client.connect();
        
        // Set read-only mode at the session level if enabled
        if (this.readOnlyMode) {
          await client.query('SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY');
          console.error('Read-only mode enabled for this connection');
        }
        
        this.connections.set(connectionKey, client);
      } catch (error) {
        throw new McpError(
          ErrorCode.InternalError,
          `Failed to connect to database: ${error instanceof Error ? error.message : 'Unknown error'}`
        );
      }
    }

    try {
      const result = await client.query(args.query);
      
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              rowCount: result.rowCount,
              rows: result.rows,
              fields: result.fields?.map(f => ({
                name: f.name,
                dataTypeID: f.dataTypeID,
              })),
            }, null, 2),
          },
        ],
      };
    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              error: true,
              message: error instanceof Error ? error.message : 'Unknown error',
            }, null, 2),
          },
        ],
      };
    }
  }

  async run() {
    const transport = new StdioServerTransport();
    await this.server.connect(transport);
    console.error(`${process.env.MCP_SERVER_NAME} server running on stdio`);
  }
}

const server = new PostgresServer();
server.run().catch(console.error);