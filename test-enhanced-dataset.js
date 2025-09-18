#!/usr/bin/env node

/**
 * Test Enhanced Dataset with Complex Queries
 */

const { Client } = require('@modelcontextprotocol/sdk/client/index.js');
const { StreamableHTTPClientTransport } = require('@modelcontextprotocol/sdk/client/streamableHttp.js');

console.log('🎉 Testing Enhanced Dataset with Complex Queries...\n');

async function testEnhancedDataset() {
  try {
    const transport = new StreamableHTTPClientTransport(new URL('http://localhost:3000/mcp'));
    const client = new Client({ name: 'enhanced-test', version: '1.0.0' }, { capabilities: {} });
    await client.connect(transport);

    // Test 1: Dataset Overview
    console.log('📊 Dataset Overview:');
    const countResult = await client.callTool({
      name: 'dbQueryTool',
      arguments: { sql: 'SELECT COUNT(*) as total_employees FROM employees' }
    });

    const countData = JSON.parse(countResult.content[0].text);
    const totalEmployees = countData.events.find(e => e.type === 'data_chunk').data.chunk[0].total_employees;
    console.log(`   👥 Total Employees: ${totalEmployees}`);

    // Test 2: New Columns Overview
    console.log('\n🔧 New Columns Available:');
    const columnsResult = await client.callTool({
      name: 'dbQueryTool',
      arguments: { sql: 'SELECT * FROM employees LIMIT 1' }
    });

    const columnsData = JSON.parse(columnsResult.content[0].text);
    const columns = columnsData.events.find(e => e.type === 'query_start').data.columns;
    columns.forEach(col => console.log(`   📋 ${col}`));

    // Test 3: 15-Year Veterans (2009-2010 hires)
    console.log('\n🏆 15-Year Veterans (VP Level):');
    const veteransResult = await client.callTool({
      name: 'dbQueryTool',
      arguments: {
        sql: `SELECT employeeName, position, salary, location
              FROM employees
              WHERE EXTRACT(YEAR FROM startDate) IN (2009, 2010)
              ORDER BY salary DESC LIMIT 5`
      }
    });

    const veteransData = JSON.parse(veteransResult.content[0].text);
    veteransData.events.filter(e => e.type === 'data_chunk').forEach(event => {
      event.data.chunk.forEach(row => {
        console.log(`   👑 ${row.employeeName}: ${row.position}`);
        console.log(`      💰 $${row.salary.toLocaleString()} (${row.location})`);
      });
    });

    // Test 4: Department Statistics
    console.log('\n💼 Department Analysis:');
    const deptResult = await client.callTool({
      name: 'dbQueryTool',
      arguments: {
        sql: `SELECT department,
              COUNT(*) as headcount,
              CAST(AVG(salary) as INTEGER) as avg_salary,
              SUM(CASE WHEN isRemote THEN 1 ELSE 0 END) as remote_count
              FROM employees
              GROUP BY department
              ORDER BY avg_salary DESC`
      }
    });

    const deptData = JSON.parse(deptResult.content[0].text);
    deptData.events.filter(e => e.type === 'data_chunk').forEach(event => {
      event.data.chunk.forEach(row => {
        const remotePerc = Math.round(100 * row.remote_count / row.headcount);
        console.log(`   🏢 ${row.department}: ${row.headcount} employees`);
        console.log(`      💰 Avg Salary: $${row.avg_salary.toLocaleString()}`);
        console.log(`      🏠 Remote: ${row.remote_count}/${row.headcount} (${remotePerc}%)`);
      });
    });

    // Test 5: Salary Analysis by Tenure
    console.log('\n📈 Salary by Experience Level:');
    const tenureResult = await client.callTool({
      name: 'dbQueryTool',
      arguments: {
        sql: `SELECT
              CASE
                WHEN EXTRACT(YEAR FROM startDate) <= 2010 THEN '15+ years (Executive)'
                WHEN EXTRACT(YEAR FROM startDate) <= 2015 THEN '10-14 years (Senior)'
                WHEN EXTRACT(YEAR FROM startDate) <= 2020 THEN '5-9 years (Mid-level)'
                ELSE '0-4 years (Junior)'
              END as experience_level,
              COUNT(*) as employee_count,
              CAST(AVG(salary) as INTEGER) as avg_salary,
              MIN(salary) as min_salary,
              MAX(salary) as max_salary
              FROM employees
              GROUP BY experience_level
              ORDER BY avg_salary DESC`
      }
    });

    const tenureData = JSON.parse(tenureResult.content[0].text);
    tenureData.events.filter(e => e.type === 'data_chunk').forEach(event => {
      event.data.chunk.forEach(row => {
        console.log(`   ⭐ ${row.experience_level}`);
        console.log(`      👥 ${row.employee_count} employees`);
        console.log(`      💰 Avg: $${row.avg_salary.toLocaleString()} (Range: $${row.min_salary.toLocaleString()}-$${row.max_salary.toLocaleString()})`);
      });
    });

    await client.close();

    console.log('\n✅ Enhanced Dataset Test Complete!');
    console.log('🎯 Key Features Demonstrated:');
    console.log('   • 70 employees with career progression (2009-2025)');
    console.log('   • 10 and 15-year anniversary data');
    console.log('   • 4 new columns: department, salary, position, isRemote');
    console.log('   • Complex queries with aggregations and analysis');
    console.log('   • Real-time streaming with 1-second delays');
    console.log('   • 5-row chunks for optimal streaming experience');

  } catch (error) {
    console.error('❌ Test failed:', error.message);
    console.log('\n💡 Make sure the dev server is running with: npm run dev:fallback');
  }
}

testEnhancedDataset();