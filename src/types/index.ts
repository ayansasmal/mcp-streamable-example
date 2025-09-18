/**
 * Employee record structure matching the CSV data
 */
export interface Employee {
  employeeId: number;
  employeeName: string;
  location: string;
  startDate: string;
}

/**
 * Database query request parameters
 */
export interface QueryRequest {
  sql: string;
  params?: any[];
}

/**
 * Database query response structure
 */
export interface QueryResponse {
  data: any[];
  columns: string[];
  rowCount: number;
  executionTime: number;
}

/**
 * Streaming query result chunk
 */
export interface QueryResultChunk {
  chunk: any[];
  isComplete: boolean;
  totalRows?: number;
  error?: string;
}

/**
 * MCP tool definition for database queries
 */
export interface DbQueryToolInput {
  sql: string;
  limit?: number;
}

/**
 * Configuration for the MCP server
 */
export interface ServerConfig {
  port: number;
  host: string;
  databasePath?: string;
  csvFilePath: string;
}