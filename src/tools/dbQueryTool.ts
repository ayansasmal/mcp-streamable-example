import { Tool } from '@modelcontextprotocol/sdk/types.js';
import { DatabaseManager } from '../database/db';
import { DbQueryToolInput } from '../types/index';

/**
 * Database query tool for MCP server
 * Provides streaming query capabilities using DuckDB
 */
export class DbQueryTool {
  private dbManager: DatabaseManager;

  constructor(dbManager: DatabaseManager) {
    this.dbManager = dbManager;
  }

  /**
   * Sleep utility for adding delays between streaming events
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Get the MCP tool definition
   */
  getToolDefinition(): Tool {
    return {
      name: 'dbQueryTool',
      description: 'Execute SQL queries against the employee database with streaming results. Only SELECT statements are allowed for security.',
      inputSchema: {
        type: 'object',
        properties: {
          sql: {
            type: 'string',
            description: 'SQL SELECT query to execute against the employees table'
          },
          limit: {
            type: 'number',
            description: 'Optional limit for the number of results (default: no limit)',
            minimum: 1,
            maximum: 10000
          }
        },
        required: ['sql'],
        additionalProperties: false
      }
    };
  }

  /**
   * Execute the database query tool with streaming results
   * This is the new method that yields results row-by-row as { type: "text", text: JSON.stringify(row) }
   */
  async streamExecute(input: DbQueryToolInput): Promise<any> {
    try {
      // Validate input
      if (!input.sql || typeof input.sql !== 'string') {
        throw new Error('SQL query is required and must be a string');
      }

      // Validate SQL query for security
      if (!this.dbManager.validateQuery(input.sql)) {
        throw new Error('Invalid SQL query. Only SELECT statements are allowed. Dangerous keywords (DROP, DELETE, UPDATE, INSERT, etc.) are not permitted.');
      }

      // Apply limit if specified
      let sql = input.sql.trim();
      if (input.limit && input.limit > 0) {
        // Check if query already has a LIMIT clause
        if (!sql.toLowerCase().includes('limit')) {
          sql += ` LIMIT ${input.limit}`;
        }
      }

      // Stream results row-by-row
      const content: Array<{ type: string; text: string }> = [];
      let rowCount = 0;

      // Execute streaming query and collect each row
      for await (const dbChunk of this.dbManager.executeStreamingQuery(sql, [], 1)) {
        for (const row of dbChunk) {
          content.push({
            type: 'text',
            text: JSON.stringify(row)
          });
          rowCount++;
        }
      }

      return {
        content: content,
        _meta: {
          totalRows: rowCount,
          streamingEnabled: true,
          timestamp: new Date().toISOString()
        }
      };

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
      throw new Error(errorMessage);
    }
  }

  /**
   * Execute the database query tool (legacy method - maintained for compatibility)
   */
  async execute(input: DbQueryToolInput): Promise<string> {
    try {
      // Validate input
      if (!input.sql || typeof input.sql !== 'string') {
        throw new Error('SQL query is required and must be a string');
      }

      // Validate SQL query for security
      if (!this.dbManager.validateQuery(input.sql)) {
        throw new Error('Invalid SQL query. Only SELECT statements are allowed. Dangerous keywords (DROP, DELETE, UPDATE, INSERT, etc.) are not permitted.');
      }

      // Apply limit if specified
      let sql = input.sql.trim();
      if (input.limit && input.limit > 0) {
        // Check if query already has a LIMIT clause
        if (!sql.toLowerCase().includes('limit')) {
          sql += ` LIMIT ${input.limit}`;
        }
      }

      // For StreamableHTTPServerTransport, we should use the generator approach
      // Collect streaming events one by one as they're generated
      const events: string[] = [];
      for await (const event of this.executeStreamingTool({ sql, ...(input.limit && { limit: input.limit }) })) {
        events.push(event);
      }

      // Return as streaming format that StreamableHTTPServerTransport can handle
      return JSON.stringify({
        success: true,
        streaming: true,
        streamChunkSize: 5,
        events: events.map(eventStr => JSON.parse(eventStr)),
        timestamp: new Date().toISOString(),
        note: 'Events generated one-by-one via async generator'
      }, null, 2);

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
      return JSON.stringify({
        error: true,
        message: errorMessage,
        data: [],
        totalRows: 0
      }, null, 2);
    }
  }

  /**
   * Execute streaming query with real-time chunks (for MCP clients that support streaming)
   */
  async *executeStreamingTool(input: DbQueryToolInput): AsyncGenerator<string, void, unknown> {
    try {
      // Validate input
      if (!input.sql || typeof input.sql !== 'string') {
        throw new Error('SQL query is required and must be a string');
      }

      // Validate SQL query for security
      if (!this.dbManager.validateQuery(input.sql)) {
        throw new Error('Invalid SQL query. Only SELECT statements are allowed. Dangerous keywords (DROP, DELETE, UPDATE, INSERT, etc.) are not permitted.');
      }

      // Apply limit if specified
      let sql = input.sql.trim();
      if (input.limit && input.limit > 0) {
        // Check if query already has a LIMIT clause
        if (!sql.toLowerCase().includes('limit')) {
          sql += ` LIMIT ${input.limit}`;
        }
      }

      // Get column information first
      const schemaResult = await this.dbManager.executeQuery('SELECT * FROM employees LIMIT 0');
      const columns = schemaResult.columns;

      const startTime = Date.now();

      console.log('Starting query execution:', sql);
      // Yield initial metadata
      yield JSON.stringify({
        type: 'query_start',
        data: {
          query: sql,
          columns: columns,
          timestamp: new Date().toISOString()
        }
      });
      console.log('Yielded query_start event with columns:', columns);

      // Add delay after query_start event
      console.log('Waiting 1 second before processing data...');
      await this.sleep(1000);

      const streamChunkSize = 5; // Stream 5 rows at a time
      const dbChunkSize = 50; // Database reads 50 rows at a time internally
      let currentStreamChunk: any[] = [];
      let totalRows = 0;
      let chunkNumber = 0;

      // Execute streaming query and yield chunks as they're ready
      for await (const dbChunk of this.dbManager.executeStreamingQuery(sql, [], dbChunkSize)) {
        // Process each row from the database chunk
        for (const row of dbChunk) {
          currentStreamChunk.push(row);
          totalRows++;

          // When we have enough rows for a stream chunk, yield it
          if (currentStreamChunk.length >= streamChunkSize) {
            chunkNumber++;
            console.log(`Yielding chunk #${chunkNumber} with ${currentStreamChunk.length} rows (total rows so far: ${totalRows})`);
            yield JSON.stringify({
              type: 'data_chunk',
              data: {
                chunk: [...currentStreamChunk],
                chunkNumber: chunkNumber,
                rowsInChunk: currentStreamChunk.length,
                totalRowsSoFar: totalRows
              }
            });

            currentStreamChunk = []; // Reset for next chunk

            // Add delay after each data chunk
            console.log('Waiting 1 second before next chunk...');
            await this.sleep(1000);
          }
        }
      }

      // Send any remaining rows as final chunk
      if (currentStreamChunk.length > 0) {
        chunkNumber++;
        console.log(`Yielding final chunk #${chunkNumber} with ${currentStreamChunk.length} rows (total rows: ${totalRows})`);
        yield JSON.stringify({
          type: 'data_chunk',
          data: {
            chunk: currentStreamChunk,
            chunkNumber: chunkNumber,
            rowsInChunk: currentStreamChunk.length,
            totalRowsSoFar: totalRows
          }
        });

        // Add delay after final chunk
        console.log('Waiting 1 second before completion...');
        await this.sleep(1000);
      }

      const executionTime = Date.now() - startTime;

      console.log(`Query completed in ${executionTime}ms`);
      // Yield completion event
      yield JSON.stringify({
        type: 'query_complete',
        data: {
          totalRows: totalRows,
          totalChunks: chunkNumber,
          executionTime: executionTime,
          completed: true
        }
      });

    } catch (error) {
      console.error('Error occurred while executing query:', error);
      yield JSON.stringify({
        type: 'query_error',
        data: {
          error: true,
          message: error instanceof Error ? error.message : 'Unknown error',
          timestamp: new Date().toISOString()
        }
      });
    }
  }


  /**
   * Get sample queries for testing
   */
  getSampleQueries(): string[] {
    return [
      'SELECT * FROM employees LIMIT 10',
      'SELECT location, COUNT(*) as employee_count FROM employees GROUP BY location ORDER BY employee_count DESC',
      'SELECT * FROM employees WHERE location = \'New York\'',
      'SELECT employeeName, location, startDate FROM employees WHERE startDate >= \'2020-01-01\' ORDER BY startDate DESC',
      'SELECT location, AVG(EXTRACT(YEAR FROM startDate)) as avg_start_year FROM employees GROUP BY location',
      'SELECT COUNT(*) as total_employees FROM employees',
      'SELECT * FROM employees WHERE employeeName LIKE \'%John%\'',
      'SELECT EXTRACT(YEAR FROM startDate) as start_year, COUNT(*) as hires FROM employees GROUP BY start_year ORDER BY start_year'
    ];
  }

  /**
   * Get table schema information
   */
  async getSchema(): Promise<string> {
    try {
      const schema = await this.dbManager.getTableSchema();
      const sampleData = await this.dbManager.getSampleEmployees(5);

      const response = {
        table: 'employees',
        schema: schema,
        sampleData: sampleData,
        sampleQueries: this.getSampleQueries()
      };

      return JSON.stringify(response, null, 2);
    } catch (error) {
      throw new Error(`Failed to get schema: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }
}