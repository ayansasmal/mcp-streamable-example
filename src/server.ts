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
 * Session management interface
 */
interface SessionInfo {
  server: Server;
  transport: StreamableHTTPServerTransport;
  createdAt: Date;
  lastAccessed: Date;
}

/**
 * Main MCP Server implementation with StreamableHttpTransport
 */
class MCPDuckDBServer {
  private dbManager: DatabaseManager;
  private dbQueryTool: DbQueryTool;
  private config: ServerConfig;
  private app: express.Application;
  private sessions: Map<string, SessionInfo> = new Map();
  private sessionTTL: number = 30 * 60 * 1000; // 30 minutes default

  constructor(config: ServerConfig) {
    this.config = config;
    this.app = express();
    this.dbManager = new DatabaseManager();
    this.dbQueryTool = new DbQueryTool(this.dbManager);
    
    // Set configurable session TTL
    this.sessionTTL = config.sessionTTL || 30 * 60 * 1000; // Default 30 minutes

    // Set up session cleanup interval
    setInterval(() => this.cleanupExpiredSessions(), 5 * 60 * 1000); // Check every 5 minutes
  }

  /**
   * Attach MCP request handlers to a server instance
   * This eliminates duplication by providing a reusable handler setup function
   */
  private attachHandlers(server: Server): void {
    // Handle list tools request
    server.setRequestHandler(ListToolsRequestSchema, async () => {
      return {
        tools: [this.dbQueryTool.getToolDefinition()]
      };
    });

    // Handle call tool request
    server.setRequestHandler(CallToolRequestSchema, async (request) => {
      const { name, arguments: args } = request.params;

      if (name === 'dbQueryTool') {
        try {
          if (!args) {
            throw new Error('Arguments are required for dbQueryTool');
          }
          
          // Use the new streaming execute method
          const result = await this.dbQueryTool.streamExecute(args as any);
          return result;
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
          return {
            content: [
              {
                type: 'text',
                text: errorMessage // Plain text instead of JSON.stringify
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
   * Clean up expired sessions
   */
  private cleanupExpiredSessions(): void {
    const now = new Date();
    const expiredSessions: string[] = [];

    for (const [sessionId, session] of this.sessions.entries()) {
      if (now.getTime() - session.lastAccessed.getTime() > this.sessionTTL) {
        expiredSessions.push(sessionId);
      }
    }

    for (const sessionId of expiredSessions) {
      console.log(`Cleaning up expired session: ${sessionId}`);
      this.sessions.delete(sessionId);
    }

    if (expiredSessions.length > 0) {
      console.log(`Cleaned up ${expiredSessions.length} expired sessions`);
    }
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

      // Simplified Express middleware for /mcp route
      this.app.use((req, res, next) => {
        if (req.path.startsWith('/mcp')) {
          // Skip all middleware processing for MCP routes - let transport handle raw body
          next();
        } else {
          // Apply standard middleware for other routes
          express.json()(req, res, next);
        }
      });

      // Add CORS headers for development
      this.app.use((req, res, next) => {
        res.header('Access-Control-Allow-Origin', '*');
        res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization, mcp-session-id');
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
          timestamp: new Date().toISOString(),
          activeSessions: this.sessions.size
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

      // DELETE endpoint for manual session termination
      this.app.delete('/mcp/:sessionId', (req, res) => {
        const sessionId = req.params.sessionId;
        
        if (this.sessions.has(sessionId)) {
          this.sessions.delete(sessionId);
          console.log(`Manually terminated session: ${sessionId}`);
          res.json({ 
            success: true, 
            message: `Session ${sessionId} terminated successfully` 
          });
        } else {
          res.status(404).json({ 
            success: false, 
            message: `Session ${sessionId} not found` 
          });
        }
      });

      // Set up HTTP route for MCP communication with enhanced session management
      this.app.post('/mcp', async (req, res) => {
        try {
          const sessionId = req.headers['mcp-session-id'] as string || crypto.randomUUID();
          const now = new Date();

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

            // Use the extracted handler attachment method to avoid duplication
            this.attachHandlers(server);

            // Connect server to transport
            await server.connect(transport);

            // Store session with enhanced tracking
            session = { 
              server, 
              transport, 
              createdAt: now, 
              lastAccessed: now 
            };
            this.sessions.set(sessionId, session);
          } else {
            console.log(`Reusing existing MCP session: ${sessionId}`);
            // Update last accessed time
            session.lastAccessed = now;
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

      console.log(`MCP server configured with StreamableHTTPTransport (Session TTL: ${this.sessionTTL / 1000}s)`);

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