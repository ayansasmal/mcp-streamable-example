#!/usr/bin/env node

/**
 * Generate sample employee data for MCP DuckDB Server testing
 * Creates data/employees.csv with 25 sample employees
 */

const fs = require('fs');
const path = require('path');

const departments = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance', 'Operations'];
const locations = ['New York', 'San Francisco', 'Chicago', 'Los Angeles', 'Miami', 'Seattle', 'Boston'];
const positions = {
  Engineering: ['Software Engineer', 'Senior Developer', 'Tech Lead', 'Staff Engineer', 'Principal Engineer'],
  Sales: ['Sales Representative', 'Account Manager', 'Sales Associate'],
  Marketing: ['Marketing Specialist', 'Content Manager', 'Marketing Manager', 'Digital Marketer'],
  HR: ['HR Specialist', 'HR Manager', 'Senior HR Specialist'],
  Finance: ['Financial Analyst', 'Senior Analyst', 'Financial Planner'],
  Operations: ['Operations Manager', 'Operations Specialist']
};

const names = [
  'John Smith', 'Sarah Johnson', 'Mike Brown', 'Lisa Davis', 'David Wilson',
  'Emily Garcia', 'James Martinez', 'Maria Rodriguez', 'Robert Taylor', 'Jennifer Anderson',
  'William Thomas', 'Susan Jackson', 'Christopher White', 'Karen Harris', 'Matthew Martin',
  'Nancy Thompson', 'Daniel Garcia', 'Helen Robinson', 'Paul Clark', 'Sandra Rodriguez',
  'Mark Lewis', 'Dorothy Lee', 'Steven Walker', 'Betty Hall', 'Kenneth Allen'
];

function randomDate(start, end) {
  return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));
}

function generateEmployee(id, name) {
  const department = departments[Math.floor(Math.random() * departments.length)];
  const location = locations[Math.floor(Math.random() * locations.length)];
  const position = positions[department][Math.floor(Math.random() * positions[department].length)];
  const isRemote = Math.random() > 0.6;
  const startDate = randomDate(new Date(2018, 0, 1), new Date(2022, 11, 31));
  const salary = Math.floor(Math.random() * 40000) + 60000; // 60k-100k
  
  // Some employees haven't been promoted yet
  const lastPromoted = Math.random() > 0.3 ? 
    randomDate(startDate, new Date()) : null;

  return {
    employeeId: id,
    employeeName: name,
    location,
    startDate: startDate.toISOString().split('T')[0],
    department,
    salary,
    position,
    isRemote,
    lastPromoted: lastPromoted ? lastPromoted.toISOString().split('T')[0] : ''
  };
}

function generateCSV() {
  const employees = names.map((name, index) => generateEmployee(index + 1, name));
  
  const headers = ['employeeId', 'employeeName', 'location', 'startDate', 'department', 'salary', 'position', 'isRemote', 'lastPromoted'];
  const csvContent = [
    headers.join(','),
    ...employees.map(emp => headers.map(header => emp[header]).join(','))
  ].join('\n');

  return csvContent;
}

function main() {
  const dataDir = path.join(__dirname, '..', 'data');
  const csvPath = path.join(dataDir, 'employees.csv');

  // Create data directory if it doesn't exist
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  // Generate and write CSV
  const csvContent = generateCSV();
  fs.writeFileSync(csvPath, csvContent);

  console.log(`✅ Generated sample employee data: ${csvPath}`);
  console.log(`📊 Created ${names.length} employee records`);
  console.log('🚀 Run `npm run dev` to start the server');
}

if (require.main === module) {
  main();
}

module.exports = { generateCSV };