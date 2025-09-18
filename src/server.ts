import express from 'express';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { CallToolRequestSchema, ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js';
import { DatabaseManager } from './database/db';
import { DbQueryTool } from './tools/dbQueryTool';
import { ServerConfig } from './types/index';
import path from 'path';
import crypto from 'crypto';

/**
 * Main MCP Server implementation with StreamableHttpTransport
 */
class MCPDuckDBServer {
  private server: Server;
  private dbManager: DatabaseManager;
  private dbQueryTool: DbQueryTool;
  private config: ServerConfig;
  private app: express.Application;
  private sessions: Map<string, { server: Server; transport: StreamableHTTPServerTransport }> = new Map();

  constructor(config: ServerConfig) {
    this.config = config;
    this.app = express();
    this.dbManager = new DatabaseManager();
    this.dbQueryTool = new DbQueryTool(this.dbManager);

    // Initialize MCP Server
    this.server = new Server(
      {
        name: 'mcp-duckdb-server',
        version: '1.0.0',
        description: 'MCP server with DuckDB integration for employee data queries'
      },
      {
        capabilities: {
          tools: {}
        }
      }
    );

    this.setupHandlers();
  }

  /**
   * Set up MCP server request handlers
   */
  private setupHandlers(): void {
    // Handle list tools request
    this.server.setRequestHandler(ListToolsRequestSchema, async () => {
      return {
        tools: [this.dbQueryTool.getToolDefinition()]
      };
    });

    // Handle call tool request
    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      const { name, arguments: args } = request.params;

      if (name === 'dbQueryTool') {
        try {
          if (!args) {
            throw new Error('Arguments are required for dbQueryTool');
          }
          const result = await this.dbQueryTool.execute(args as any);
          return {
            content: [
              {
                type: 'text',
                text: result
              }
            ]
          };
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify({
                  error: true,
                  message: errorMessage,
                  timestamp: new Date().toISOString()
                }, null, 2)
              }
            ],
            isError: true
          };
        }
      }

      throw new Error(`Unknown tool: ${name}`);
    });
  }

  /**
   * Initialize the server
   */
  async initialize(): Promise<void> {
    try {
      console.log('Initializing MCP DuckDB Server...');

      // Initialize database
      await this.dbManager.initialize(this.config.csvFilePath);
      console.log('Database initialized successfully with expanded dataset (70 employees)');

      // Set up Express middleware for non-MCP routes
      this.app.use((req, res, next) => {
        if (req.path.startsWith('/mcp')) {
          // Skip JSON parsing for MCP routes - let transport handle raw body
          next();
        } else {
          express.json()(req, res, next);
        }
      });
      this.app.use(express.urlencoded({ extended: true }));

      // Add CORS headers for development
      this.app.use((req, res, next) => {
        res.header('Access-Control-Allow-Origin', '*');
        res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
        res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
        if (req.method === 'OPTIONS') {
          res.sendStatus(200);
        } else {
          next();
        }
      });

      // Health check endpoint
      this.app.get('/health', (_req, res) => {
        res.json({
          status: 'healthy',
          server: 'mcp-duckdb-server',
          version: '1.0.0',
          timestamp: new Date().toISOString()
        });
      });

      // Schema endpoint for debugging
      this.app.get('/schema', async (_req, res) => {
        try {
          const schema = await this.dbQueryTool.getSchema();
          res.json(JSON.parse(schema));
        } catch (error) {
          res.status(500).json({
            error: 'Failed to get schema',
            message: error instanceof Error ? error.message : 'Unknown error'
          });
        }
      });

      // Set up HTTP route for MCP communication with session management
      this.app.post('/mcp', async (req, res) => {
        try {
          const sessionId = req.headers['mcp-session-id'] as string || crypto.randomUUID();

          let session = this.sessions.get(sessionId);

          if (!session) {
            // Create new session
            console.log(`Creating new MCP session: ${sessionId}`);

            const transport = new StreamableHTTPServerTransport({
              sessionIdGenerator: () => sessionId
            });

            const server = new Server(
              {
                name: 'mcp-duckdb-server',
                version: '1.0.0',
                description: 'MCP server with DuckDB integration for employee data queries'
              },
              {
                capabilities: {
                  tools: {}
                }
              }
            );

            // Set up handlers for this session's server
            server.setRequestHandler(ListToolsRequestSchema, async () => {
              return {
                tools: [this.dbQueryTool.getToolDefinition()]
              };
            });

            server.setRequestHandler(CallToolRequestSchema, async (request) => {
              const { name, arguments: args } = request.params;

              if (name === 'dbQueryTool') {
                try {
                  if (!args) {
                    throw new Error('Arguments are required for dbQueryTool');
                  }
                  const result = await this.dbQueryTool.execute(args as any);
                  return {
                    content: [
                      {
                        type: 'text',
                        text: result
                      }
                    ]
                  };
                } catch (error) {
                  const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
                  return {
                    content: [
                      {
                        type: 'text',
                        text: JSON.stringify({
                          error: true,
                          message: errorMessage,
                          timestamp: new Date().toISOString()
                        }, null, 2)
                      }
                    ],
                    isError: true
                  };
                }
              }

              throw new Error(`Unknown tool: ${name}`);
            });

            // Connect server to transport
            await server.connect(transport);

            // Store session
            session = { server, transport };
            this.sessions.set(sessionId, session);

            // Clean up session after some time (optional)
            setTimeout(() => {
              console.log(`Cleaning up session: ${sessionId}`);
              this.sessions.delete(sessionId);
            }, 30 * 60 * 1000); // 30 minutes
          } else {
            console.log(`Reusing existing MCP session: ${sessionId}`);
          }

          // Handle the request with the session's transport
          await session.transport.handleRequest(req, res);

        } catch (error) {
          console.error('MCP request handling error:', error);
          if (!res.headersSent) {
            res.status(500).json({
              jsonrpc: '2.0',
              error: {
                code: -32603,
                message: 'Internal error',
                data: error instanceof Error ? error.message : 'Unknown error'
              }
            });
          }
        }
      });

      console.log('MCP server configured with StreamableHTTPTransport');

    } catch (error) {
      console.error('Failed to initialize server:', error);
      throw error;
    }
  }

  /**
   * Start the HTTP server
   */
  async start(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        const httpServer = this.app.listen(this.config.port, this.config.host, () => {
          console.log(`🚀 MCP DuckDB Server running on http://${this.config.host}:${this.config.port}`);
          console.log(`📊 MCP endpoint available at http://${this.config.host}:${this.config.port}/mcp`);
          console.log(`💚 Health check at http://${this.config.host}:${this.config.port}/health`);
          console.log(`📋 Schema info at http://${this.config.host}:${this.config.port}/schema`);
          resolve();
        });

        // Graceful shutdown
        process.on('SIGINT', async () => {
          console.log('\\n🔄 Gracefully shutting down...');
          httpServer.close(async () => {
            await this.dbManager.close();
            console.log('✅ Server shutdown complete');
            process.exit(0);
          });
        });

        process.on('SIGTERM', async () => {
          console.log('\\n🔄 Gracefully shutting down...');
          httpServer.close(async () => {
            await this.dbManager.close();
            console.log('✅ Server shutdown complete');
            process.exit(0);
          });
        });

      } catch (error) {
        reject(error);
      }
    });
  }

  /**
   * Get server information
   */
  getServerInfo() {
    return {
      name: 'mcp-duckdb-server',
      version: '1.0.0',
      config: this.config,
      endpoints: {
        mcp: `http://${this.config.host}:${this.config.port}/mcp`,
        health: `http://${this.config.host}:${this.config.port}/health`,
        schema: `http://${this.config.host}:${this.config.port}/schema`
      }
    };
  }
}

/**
 * Main entry point
 */
async function main() {
  try {
    // Configuration
    const config: ServerConfig = {
      port: parseInt(process.env.PORT || '3000'),
      host: process.env.HOST || 'localhost',
      csvFilePath: path.join(process.cwd(), 'data', 'employees.csv')
    };

    console.log('Starting MCP DuckDB Server with config:', config);

    // Create and start server
    const server = new MCPDuckDBServer(config);
    await server.initialize();
    await server.start();

  } catch (error) {
    console.error('❌ Failed to start server:', error);
    process.exit(1);
  }
}

// Start the server if this file is run directly
if (process.argv[1] && process.argv[1].endsWith('server.ts') || process.argv[1]?.endsWith('server.js')) {
  main();
}

export { MCPDuckDBServer };