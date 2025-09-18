import { DuckDBInstance, DuckDBConnection } from '@duckdb/node-api';
import { promises as fs } from 'fs';
import path from 'path';
import { QueryResponse, Employee } from '../types/index';

/**
 * DuckDB database manager for handling CSV data operations
 */
export class DatabaseManager {
  private instance: DuckDBInstance | null = null;
  private connection: DuckDBConnection | null = null;
  private initialized: boolean = false;

  constructor() {
    // Instance will be created in initialize()
  }

  /**
   * Initialize the database and load CSV data
   */
  async initialize(csvFilePath: string): Promise<void> {
    if (this.initialized) {
      return;
    }

    try {
      // Verify CSV file exists
      await fs.access(csvFilePath);

      // Create DuckDB instance and connection
      this.instance = await DuckDBInstance.create();
      this.connection = await this.instance.connect();

      // Create table and load CSV data
      await this.createEmployeesTable(csvFilePath);

      this.initialized = true;
      console.log('Database initialized successfully');
    } catch (error) {
      console.error('Failed to initialize database:', error);
      throw error;
    }
  }

  /**
   * Create employees table and load CSV data
   */
  private async createEmployeesTable(csvFilePath: string): Promise<void> {
    if (!this.connection) {
      throw new Error('Database connection not established');
    }

    // Create table structure
    await this.connection.run(`
      CREATE TABLE employees (
        employeeId INTEGER,
        employeeName VARCHAR,
        location VARCHAR,
        startDate DATE,
        department VARCHAR,
        salary BIGINT,
        position VARCHAR,
        isRemote BOOLEAN,
        lastPromoted DATE
      )
    `);

    // Load CSV data into the table
    const absolutePath = path.resolve(csvFilePath);
    await this.connection.run(`
      COPY employees FROM '${absolutePath}' (FORMAT CSV, HEADER TRUE)
    `);
  }

  /**
   * Convert BigInt values to regular numbers and format dates for JSON serialization
   */
  private convertBigIntsToNumbers(obj: any): any {
    if (typeof obj === 'bigint') {
      return Number(obj);
    } else if (Array.isArray(obj)) {
      return obj.map(item => this.convertBigIntsToNumbers(item));
    } else if (obj !== null && typeof obj === 'object') {
      const converted: any = {};
      for (const key in obj) {
        const value = obj[key];
        // Handle DuckDB date objects that have a 'days' property
        if (value && typeof value === 'object' && 'days' in value && typeof value.days === 'number') {
          // Convert DuckDB date to ISO string format
          const epochDate = new Date(1970, 0, 1); // January 1, 1970
          epochDate.setDate(epochDate.getDate() + value.days);
          converted[key] = epochDate.toISOString().split('T')[0]; // Format as YYYY-MM-DD
        } else {
          converted[key] = this.convertBigIntsToNumbers(value);
        }
      }
      return converted;
    }
    return obj;
  }

  /**
   * Execute a SQL query with optional streaming
   */
  async executeQuery(sql: string, _params: any[] = []): Promise<QueryResponse> {
    if (!this.initialized || !this.connection) {
      throw new Error('Database not initialized');
    }

    const startTime = Date.now();

    try {
      const reader = await this.connection.runAndReadAll(sql);
      const rawRows = reader.getRowObjects();
      const executionTime = Date.now() - startTime;
      const columns = reader.columnNames();

      // Convert BigInts to numbers for JSON serialization
      const rows = this.convertBigIntsToNumbers(rawRows);

      const response: QueryResponse = {
        data: rows,
        columns: columns,
        rowCount: rows.length,
        executionTime
      };

      return response;
    } catch (error) {
      throw error;
    }
  }

  /**
   * Execute a streaming query that yields results in chunks
   */
  async *executeStreamingQuery(sql: string, _params: any[] = [], chunkSize: number = 100): AsyncGenerator<any[], void, unknown> {
    if (!this.initialized || !this.connection) {
      throw new Error('Database not initialized');
    }

    try {
      const reader = await this.connection.runAndReadAll(sql);
      const rawRows = reader.getRowObjects();

      // Convert BigInts to numbers for JSON serialization
      const rows = this.convertBigIntsToNumbers(rawRows);

      // Yield results in chunks
      for (let i = 0; i < rows.length; i += chunkSize) {
        const chunk = rows.slice(i, i + chunkSize);
        yield chunk;
      }
    } catch (error) {
      throw error;
    }
  }

  /**
   * Get table schema information
   */
  async getTableSchema(tableName: string = 'employees'): Promise<any[]> {
    const sql = `SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '${tableName}'`;
    try {
      const result = await this.executeQuery(sql);
      return result.data;
    } catch {
      // Fallback: get column info from LIMIT 0 query
      const fallbackSql = `SELECT * FROM ${tableName} LIMIT 0`;
      const result = await this.executeQuery(fallbackSql);
      return result.columns.map((col, index) => ({
        column_name: col,
        ordinal_position: index + 1,
        data_type: 'unknown'
      }));
    }
  }

  /**
   * Get sample data from employees table
   */
  async getSampleEmployees(limit: number = 10): Promise<Employee[]> {
    const sql = `SELECT * FROM employees LIMIT ${limit}`;
    const result = await this.executeQuery(sql);
    return result.data as Employee[];
  }

  /**
   * Validate SQL query for security (basic validation)
   */
  validateQuery(sql: string): boolean {
    const sanitizedSql = sql.toLowerCase().trim();

    // Only allow SELECT statements
    if (!sanitizedSql.startsWith('select')) {
      return false;
    }

    // Block potentially dangerous keywords
    const dangerousKeywords = ['drop', 'delete', 'update', 'insert', 'alter', 'create', 'truncate'];
    for (const keyword of dangerousKeywords) {
      if (sanitizedSql.includes(keyword)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Close database connection
   */
  async close(): Promise<void> {
    try {
      if (this.connection) {
        this.connection.closeSync();
        this.connection = null;
      }
      if (this.instance) {
        // DuckDBInstance doesn't have a close method in the Neo API
        this.instance = null;
      }
      this.initialized = false;
    } catch (error) {
      console.error('Error closing database:', error);
    }
  }
}