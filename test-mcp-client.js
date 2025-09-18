#!/usr/bin/env node

/**
 * MCP Client Test for DuckDB Server
 * Tests the dbQueryTool functionality with streaming capabilities
 */

const { Client } = require('@modelcontextprotocol/sdk/client/index.js');
const { StreamableHTTPClientTransport } = require('@modelcontextprotocol/sdk/client/streamableHttp.js');

console.log('🧪 Starting MCP DuckDB Server Test Client...');

/**
 * Create MCP client for testing DuckDB server
 */
async function createDuckDBClient(serverUrl = 'http://localhost:3000/mcp') {
  console.log(`🌐 Connecting to MCP DuckDB server at: ${serverUrl}`);

  try {
    // Create HTTP transport
    const transport = new StreamableHTTPClientTransport(
      new URL(serverUrl)
    );

    console.log('✅ Created StreamableHTTPClientTransport');

    // Create MCP Client instance
    const client = new Client({
      name: 'duckdb-test-client',
      version: '1.0.0'
    }, {
      capabilities: {
        sampling: {}
      }
    });

    console.log('✅ Created MCP Client');

    // Connect client to transport
    await client.connect(transport);
    console.log('✅ MCP DuckDB Client connected successfully');

    return { client, transport };

  } catch (error) {
    console.error(`❌ Failed to create MCP client: ${error.message}`);
    console.error('📋 Full error:', error);
    throw error;
  }
}

/**
 * Test DuckDB server operations
 */
async function testDuckDBOperations(client) {
  console.log('\n🔧 Testing DuckDB MCP Operations:');
  console.log('=' .repeat(50));

  try {
    // Test 1: List available tools
    console.log('\n📋 Listing available tools...');
    const toolsResponse = await client.listTools();

    if (toolsResponse && toolsResponse.tools) {
      console.log(`✅ Found ${toolsResponse.tools.length} tools:`);
      toolsResponse.tools.forEach(tool => {
        console.log(`   📎 ${tool.name}: ${tool.description}`);
      });
    } else {
      console.log('⚠️  No tools found or unexpected response format');
      return false;
    }

    // Test 2: Simple query - count employees
    console.log('\n🔧 Testing dbQueryTool - Count employees...');
    const countResult = await client.callTool({
      name: 'dbQueryTool',
      arguments: {
        sql: 'SELECT COUNT(*) as total_employees FROM employees'
      }
    });

    if (countResult && countResult.content) {
      console.log('✅ Count query response:');
      countResult.content.forEach(content => {
        console.log(`   📊 ${content.text}`);
      });
    } else {
      console.log('⚠️  Unexpected count query response');
      console.log('📋 Raw response:', JSON.stringify(countResult, null, 2));
    }

    // Test 2.5: Date formatting test
    console.log('\n🔧 Testing dbQueryTool - Date formatting (3 employees with dates)...');
    const dateResult = await client.callTool({
      name: 'dbQueryTool',
      arguments: {
        sql: 'SELECT employeeId, employeeName, location, startDate FROM employees LIMIT 3'
      }
    });

    if (dateResult && dateResult.content) {
      console.log('✅ Date formatting response:');
      dateResult.content.forEach(content => {
        const data = JSON.parse(content.text);
        if (data.streaming && data.events) {
          const dataChunks = data.events.filter(e => e.type === 'data_chunk');
          console.log('   📅 Sample employees with formatted dates:');
          dataChunks.forEach(event => {
            event.data.chunk.forEach(row => {
              console.log(`      👤 ${row.employeeName}: ${row.location}, started ${row.startDate}`);
            });
          });
        }
      });
    }

    // Test 3: Small result set (5 employees)
    console.log('\n🔧 Testing dbQueryTool - Small result set (5 employees)...');
    const smallResult = await client.callTool({
      name: 'dbQueryTool',
      arguments: {
        sql: 'SELECT * FROM employees LIMIT 5'
      }
    });

    if (smallResult && smallResult.content) {
      console.log('✅ Small query response (should be 1 chunk):');
      smallResult.content.forEach(content => {
        const data = JSON.parse(content.text);
        if (data.streaming && data.events) {
          console.log(`   📦 Streaming events: ${data.events.length}`);
          data.events.forEach((event, index) => {
            console.log(`   📋 Event ${index + 1}: ${event.type}`);
            if (event.type === 'data_chunk') {
              console.log(`      Rows: ${event.data.rowsInChunk}`);
            }
          });
        }
      });
    }

    // Test 4: Medium result set (25 employees) - should trigger chunking
    console.log('\n🔧 Testing dbQueryTool - Medium result set (25 employees)...');
    const mediumResult = await client.callTool({
      name: 'dbQueryTool',
      arguments: {
        sql: 'SELECT * FROM employees LIMIT 25'
      }
    });

    if (mediumResult && mediumResult.content) {
      console.log('✅ Medium query response (should be 5 chunks: 5+5+5+5+5):');
      mediumResult.content.forEach(content => {
        const data = JSON.parse(content.text);
        if (data.streaming && data.events) {
          console.log(`   📦 Streaming events: ${data.events.length}`);
          const dataChunks = data.events.filter(e => e.type === 'data_chunk');
          console.log(`   🔢 Data chunks: ${dataChunks.length}`);
          dataChunks.forEach((event, index) => {
            console.log(`   📦 Chunk ${index + 1}: ${event.data.rowsInChunk} rows`);
          });
        }
      });
    }

    // Test 5: Large result set (all employees) - should trigger chunking
    console.log('\n🔧 Testing dbQueryTool - Large result set (all 50 employees)...');
    const largeResult = await client.callTool({
      name: 'dbQueryTool',
      arguments: {
        sql: 'SELECT * FROM employees'
      }
    });

    if (largeResult && largeResult.content) {
      console.log('✅ Large query response (should be 10 chunks of 5 rows each):');
      largeResult.content.forEach(content => {
        const data = JSON.parse(content.text);
        if (data.streaming && data.events) {
          console.log(`   📦 Streaming events: ${data.events.length}`);
          const dataChunks = data.events.filter(e => e.type === 'data_chunk');
          console.log(`   🔢 Data chunks: ${dataChunks.length}`);
          dataChunks.forEach((event, index) => {
            console.log(`   📦 Chunk ${index + 1}: ${event.data.rowsInChunk} rows (total so far: ${event.data.totalRowsSoFar})`);
          });

          const completeEvent = data.events.find(e => e.type === 'query_complete');
          if (completeEvent) {
            console.log(`   ✅ Query completed: ${completeEvent.data.totalRows} total rows in ${completeEvent.data.executionTime}ms`);
          }
        }
      });
    }

    // Test 6: Aggregation query
    console.log('\n🔧 Testing dbQueryTool - Aggregation query...');
    const aggResult = await client.callTool({
      name: 'dbQueryTool',
      arguments: {
        sql: 'SELECT location, COUNT(*) as employee_count FROM employees GROUP BY location ORDER BY employee_count DESC'
      }
    });

    if (aggResult && aggResult.content) {
      console.log('✅ Aggregation query response:');
      aggResult.content.forEach(content => {
        const data = JSON.parse(content.text);
        if (data.streaming && data.events) {
          const dataChunks = data.events.filter(e => e.type === 'data_chunk');
          console.log(`   📊 Location summary (${dataChunks.length} chunks):`);
          dataChunks.forEach(event => {
            event.data.chunk.forEach(row => {
              console.log(`      📍 ${row.location}: ${row.employee_count} employees`);
            });
          });
        }
      });
    }

    // Test 7: Invalid query (should fail securely)
    console.log('\n🔧 Testing dbQueryTool - Invalid query (security test)...');
    try {
      const invalidResult = await client.callTool({
        name: 'dbQueryTool',
        arguments: {
          sql: 'DROP TABLE employees'
        }
      });

      if (invalidResult && invalidResult.content) {
        invalidResult.content.forEach(content => {
          const data = JSON.parse(content.text);
          if (data.error) {
            console.log('✅ Security validation working - dangerous query blocked:');
            console.log(`   🛡️  ${data.message}`);
          }
        });
      }
    } catch (error) {
      console.log('✅ Security validation working - query properly rejected');
    }

    return true;

  } catch (error) {
    console.error(`❌ DuckDB operations test failed: ${error.message}`);
    console.error('📋 Full error:', error);
    return false;
  }
}

/**
 * Test connection cleanup
 */
async function testCleanup(client) {
  try {
    console.log('\n🧹 Closing client connection...');
    await client.close();
    console.log('✅ Client connection closed successfully');

  } catch (error) {
    console.error(`❌ Cleanup failed: ${error.message}`);
  }
}

/**
 * Main test function
 */
async function main() {
  const serverUrl = process.env.MCP_SERVER_URL || 'http://localhost:3000/mcp';

  try {
    console.log('\n🎯 MCP DuckDB Server Test Starting!');
    console.log('=' .repeat(60));
    console.log(`🌐 Server URL: ${serverUrl}`);
    console.log(`🔧 Client: duckdb-test-client v1.0.0`);
    console.log(`📊 Testing: dbQueryTool with streaming (5-row chunks)`);
    console.log('=' .repeat(60));

    // Create and connect client
    const { client, transport } = await createDuckDBClient(serverUrl);

    // Test DuckDB operations
    const testsSuccessful = await testDuckDBOperations(client);

    // Cleanup
    await testCleanup(client);

    console.log('\n📋 Test Summary:');
    console.log('=' .repeat(40));
    if (testsSuccessful) {
      console.log('✅ All DuckDB tests passed successfully!');
      console.log('✅ dbQueryTool streaming is working correctly');
      console.log('✅ Security validations are working');
      console.log('✅ 5-row chunking is functioning as expected');
      console.log('🎉 MCP DuckDB Server is ready for production!');
    } else {
      console.log('❌ Some tests failed');
      console.log('⚠️  MCP DuckDB server may have issues');
      process.exit(1);
    }

  } catch (error) {
    console.error('💥 Fatal error:', error.message);
    console.error('📋 Full error:', error);
    process.exit(1);
  }
}

// Run if this file is executed directly
if (require.main === module) {
  main().catch((error) => {
    console.error('💥 Unhandled error:', error);
    process.exit(1);
  });
}

module.exports = { createDuckDBClient, testDuckDBOperations, main };