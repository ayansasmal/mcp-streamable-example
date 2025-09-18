# Copilot Instructions for MCP DuckDB Server

## Project Overview

This repository implements a Model Context Protocol (MCP) server in TypeScript/Node.js, integrating DuckDB for fast analytics on employee data. The server streams query results in real-time using StreamableHTTPServerTransport, making it ideal for LLM and agent integration.

## Architecture & Data Flow

- **src/server.ts**: Main entry point. Sets up Express HTTP server, MCP protocol, and streaming transport.
- **src/tools/dbQueryTool.ts**: Implements the `dbQueryTool` for SQL queries (SELECT only) against DuckDB, streaming results in 10-row chunks.
- **src/database/db.ts**: Handles DuckDB connection, loads `data/employees.csv` into memory, manages queries and schema.
- **data/employees.csv**: Sample dataset (70 employees, 9 columns) for analytics and demo queries.

### Streaming Query Flow

1. Client POSTs to `/mcp` endpoint with a dbQueryTool call.
2. Server validates SQL (SELECT only), executes query in DuckDB.
3. Results streamed in 10-row JSON events (`data_chunk`) via HTTP/SSE.
4. Events: `query_start`, `data_chunk`, `query_complete`, error events.

## Developer Workflows

- **Install dependencies**: `npm install`
- **Start dev server (native TS)**: `npm run dev`
- **Start dev server (fallback)**: `npm run dev:fallback`
- **Build for production**: `npm run build`
- **Run production build**: `npm run start`
- **Test streaming**: `npm run test:streaming`
- **Manual endpoint tests**: `curl http://localhost:3000/health` or `/schema`

## Key Conventions & Patterns

- Only SELECT queries allowed; all others blocked for security.
- Query results streamed in 10-row chunks (configurable in code).
- SQL and input validation enforced in `dbQueryTool`.
- DuckDB is loaded in-memory from CSV at startup.
- All tool calls use JSON schema defined in `dbQueryTool.ts`.
- TypeScript types for queries and responses in `src/types/index.ts`.

## Integration Points

- MCP protocol via `@modelcontextprotocol/sdk`.
- DuckDB via `@duckdb/node-api`.
- Express for HTTP/SSE endpoints.
- LLM/agent integration: natural language mapped to SQL, demo flows in README.

## Example Tool Call

```json
{
  "name": "dbQueryTool",
  "arguments": {
    "sql": "SELECT * FROM employees WHERE location = 'New York'",
    "limit": 50
  }
}
```

## References

- See `README.md` for architecture diagrams, event types, and sample queries.
- See `CLAUDE.md` for additional agent guidance.
- Key files: `src/server.ts`, `src/tools/dbQueryTool.ts`, `src/database/db.ts`, `data/employees.csv`

---

For questions, review the MCP documentation or open an issue.
