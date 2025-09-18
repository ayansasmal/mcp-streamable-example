# Complex MCP DuckDB Query Examples

## Enhanced Dataset Overview
- **70 employees** total
- **10-15 year veterans** (2009-2010 hires): 10 employees with VP/Executive positions
- **10 year veterans** (2014-2015 hires): 10 employees with Principal/Director positions
- **Recent hires** (2018-2025): 50 employees with various levels
- **New columns**: department, salary, position, isRemote

## 1. Anniversary and Tenure Analysis

### 15-Year Anniversary Recipients (Hired 2009-2010)
```sql
SELECT employeeName, location, startDate, position, salary,
       EXTRACT(YEAR FROM '2025-01-01') - EXTRACT(YEAR FROM startDate) as years_of_service
FROM employees
WHERE EXTRACT(YEAR FROM startDate) IN (2009, 2010)
ORDER BY startDate;
```

### 10-Year Anniversary Recipients (Hired 2014-2015)
```sql
SELECT employeeName, department, position, salary, location,
       DATE_DIFF('year', startDate, '2025-01-01') as tenure_years
FROM employees
WHERE EXTRACT(YEAR FROM startDate) IN (2014, 2015)
ORDER BY salary DESC;
```

### Tenure Distribution Analysis
```sql
SELECT
  CASE
    WHEN EXTRACT(YEAR FROM startDate) <= 2010 THEN '15+ years'
    WHEN EXTRACT(YEAR FROM startDate) <= 2015 THEN '10-14 years'
    WHEN EXTRACT(YEAR FROM startDate) <= 2020 THEN '5-9 years'
    ELSE '0-4 years'
  END as tenure_group,
  COUNT(*) as employee_count,
  AVG(salary) as avg_salary,
  MIN(salary) as min_salary,
  MAX(salary) as max_salary
FROM employees
GROUP BY tenure_group
ORDER BY AVG(salary) DESC;
```

## 2. Salary and Compensation Analysis

### Salary Statistics by Department
```sql
SELECT department,
       COUNT(*) as headcount,
       AVG(salary) as avg_salary,
       MEDIAN(salary) as median_salary,
       MIN(salary) as min_salary,
       MAX(salary) as max_salary,
       STDDEV(salary) as salary_stddev
FROM employees
GROUP BY department
ORDER BY avg_salary DESC;
```

### Top Earners Analysis
```sql
SELECT employeeName, department, position, salary, location, isRemote,
       EXTRACT(YEAR FROM '2025-01-01') - EXTRACT(YEAR FROM startDate) as tenure_years,
       salary / (EXTRACT(YEAR FROM '2025-01-01') - EXTRACT(YEAR FROM startDate)) as salary_per_year_tenure
FROM employees
WHERE salary >= 130000
ORDER BY salary DESC;
```

### Salary Quartiles by Position Level
```sql
SELECT position,
       COUNT(*) as count,
       PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary) as q1_salary,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) as median_salary,
       PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary) as q3_salary,
       AVG(salary) as mean_salary
FROM employees
GROUP BY position
HAVING COUNT(*) >= 3
ORDER BY median_salary DESC;
```

## 3. Remote Work Analysis

### Remote Work Distribution
```sql
SELECT location,
       COUNT(*) as total_employees,
       SUM(CASE WHEN isRemote THEN 1 ELSE 0 END) as remote_employees,
       ROUND(100.0 * SUM(CASE WHEN isRemote THEN 1 ELSE 0 END) / COUNT(*), 2) as remote_percentage
FROM employees
GROUP BY location
HAVING COUNT(*) >= 5
ORDER BY remote_percentage DESC;
```

### Remote vs Office Salary Comparison
```sql
SELECT department,
       isRemote,
       COUNT(*) as employee_count,
       AVG(salary) as avg_salary,
       MEDIAN(salary) as median_salary
FROM employees
GROUP BY department, isRemote
ORDER BY department, isRemote DESC;
```

## 4. Department and Position Analysis

### Engineering Career Progression
```sql
SELECT position,
       COUNT(*) as count,
       AVG(salary) as avg_salary,
       AVG(EXTRACT(YEAR FROM '2025-01-01') - EXTRACT(YEAR FROM startDate)) as avg_tenure,
       STRING_AGG(location, ', ' ORDER BY salary DESC) as top_locations
FROM employees
WHERE department = 'Engineering'
GROUP BY position
ORDER BY avg_salary DESC;
```

### Department Growth by Hiring Year
```sql
SELECT EXTRACT(YEAR FROM startDate) as hire_year,
       department,
       COUNT(*) as hires,
       AVG(salary) as avg_starting_salary
FROM employees
GROUP BY hire_year, department
ORDER BY hire_year DESC, hires DESC;
```

## 5. Geographic and Cost Analysis

### Cost Per Employee by State
```sql
SELECT location,
       COUNT(*) as employee_count,
       SUM(salary) as total_payroll,
       AVG(salary) as avg_salary,
       SUM(salary) / COUNT(*) as cost_per_employee
FROM employees
GROUP BY location
ORDER BY total_payroll DESC;
```

### High-Value States Analysis
```sql
SELECT location,
       COUNT(*) as employee_count,
       SUM(CASE WHEN salary >= 120000 THEN 1 ELSE 0 END) as high_earners,
       ROUND(100.0 * SUM(CASE WHEN salary >= 120000 THEN 1 ELSE 0 END) / COUNT(*), 2) as high_earner_percentage,
       AVG(salary) as avg_salary
FROM employees
GROUP BY location
HAVING COUNT(*) >= 5
ORDER BY high_earner_percentage DESC;
```

## 6. Complex Multi-Table Style Analysis

### Executive vs Individual Contributor Comparison
```sql
SELECT
  CASE
    WHEN position LIKE '%VP%' OR position LIKE '%Director%' OR position = 'CFO' THEN 'Executive'
    WHEN position LIKE '%Manager%' OR position LIKE '%Lead%' OR position LIKE '%Principal%' THEN 'Management'
    ELSE 'Individual Contributor'
  END as role_category,
  COUNT(*) as count,
  AVG(salary) as avg_salary,
  AVG(EXTRACT(YEAR FROM '2025-01-01') - EXTRACT(YEAR FROM startDate)) as avg_tenure,
  ROUND(100.0 * SUM(CASE WHEN isRemote THEN 1 ELSE 0 END) / COUNT(*), 2) as remote_percentage
FROM employees
GROUP BY role_category
ORDER BY avg_salary DESC;
```

### Hiring Trends and Compensation Growth
```sql
SELECT EXTRACT(YEAR FROM startDate) as hire_year,
       COUNT(*) as hires,
       AVG(salary) as avg_salary,
       LAG(AVG(salary)) OVER (ORDER BY EXTRACT(YEAR FROM startDate)) as prev_year_avg,
       ROUND(100.0 * (AVG(salary) - LAG(AVG(salary)) OVER (ORDER BY EXTRACT(YEAR FROM startDate))) /
             LAG(AVG(salary)) OVER (ORDER BY EXTRACT(YEAR FROM startDate)), 2) as salary_growth_pct
FROM employees
WHERE EXTRACT(YEAR FROM startDate) >= 2018
GROUP BY hire_year
ORDER BY hire_year;
```

## 7. Advanced Analytics Queries

### Employee Retention Cohort Analysis
```sql
SELECT
  EXTRACT(YEAR FROM startDate) as cohort_year,
  COUNT(*) as cohort_size,
  AVG(EXTRACT(YEAR FROM '2025-01-01') - EXTRACT(YEAR FROM startDate)) as avg_tenure,
  COUNT(CASE WHEN EXTRACT(YEAR FROM '2025-01-01') - EXTRACT(YEAR FROM startDate) >= 5 THEN 1 END) as five_year_retention,
  ROUND(100.0 * COUNT(CASE WHEN EXTRACT(YEAR FROM '2025-01-01') - EXTRACT(YEAR FROM startDate) >= 5 THEN 1 END) / COUNT(*), 2) as retention_rate
FROM employees
GROUP BY cohort_year
HAVING COUNT(*) >= 3
ORDER BY cohort_year;
```

### Salary Outlier Detection
```sql
WITH salary_stats AS (
  SELECT department,
         AVG(salary) as dept_avg,
         STDDEV(salary) as dept_stddev
  FROM employees
  GROUP BY department
)
SELECT e.employeeName, e.department, e.position, e.salary, e.location,
       s.dept_avg,
       ROUND((e.salary - s.dept_avg) / s.dept_stddev, 2) as z_score,
       CASE
         WHEN ABS((e.salary - s.dept_avg) / s.dept_stddev) > 2 THEN 'Outlier'
         WHEN ABS((e.salary - s.dept_avg) / s.dept_stddev) > 1.5 THEN 'Notable'
         ELSE 'Normal'
       END as salary_category
FROM employees e
JOIN salary_stats s ON e.department = s.department
ORDER BY ABS((e.salary - s.dept_avg) / s.dept_stddev) DESC;
```

## 8. Promotion and Career Development Analysis

### Promotion-Ready Candidates (2+ years since last promotion)
```sql
SELECT employeeName, department, position, salary, location, isRemote,
       EXTRACT(YEAR FROM '2025-01-01') - EXTRACT(YEAR FROM startDate) as tenure_years,
       CASE
         WHEN lastPromoted IS NULL THEN DATE_DIFF('year', startDate, '2025-01-01')
         ELSE DATE_DIFF('year', lastPromoted, '2025-01-01')
       END as years_since_promotion,
       CASE
         WHEN lastPromoted IS NULL AND DATE_DIFF('year', startDate, '2025-01-01') >= 3 THEN 'High Priority - Never Promoted'
         WHEN DATE_DIFF('year', lastPromoted, '2025-01-01') >= 3 THEN 'High Priority - 3+ Years'
         WHEN DATE_DIFF('year', lastPromoted, '2025-01-01') >= 2 THEN 'Medium Priority - 2+ Years'
         ELSE 'Recently Promoted'
       END as promotion_priority
FROM employees
WHERE (lastPromoted IS NULL AND DATE_DIFF('year', startDate, '2025-01-01') >= 2)
   OR DATE_DIFF('year', lastPromoted, '2025-01-01') >= 2
ORDER BY years_since_promotion DESC, salary ASC;
```

### Department Promotion Velocity Analysis
```sql
SELECT department,
       COUNT(*) as total_employees,
       COUNT(lastPromoted) as employees_promoted,
       ROUND(100.0 * COUNT(lastPromoted) / COUNT(*), 2) as promotion_rate_pct,
       AVG(CASE WHEN lastPromoted IS NOT NULL
           THEN DATE_DIFF('year', lastPromoted, '2025-01-01') END) as avg_years_since_promotion,
       COUNT(CASE WHEN lastPromoted IS NULL AND DATE_DIFF('year', startDate, '2025-01-01') >= 2
            THEN 1 END) as never_promoted_veterans
FROM employees
GROUP BY department
ORDER BY promotion_rate_pct DESC;
```

### Salary vs Promotion Timeline Analysis
```sql
SELECT
  CASE
    WHEN salary >= 140000 THEN 'Executive ($140K+)'
    WHEN salary >= 120000 THEN 'Senior ($120K-$139K)'
    WHEN salary >= 100000 THEN 'Mid-level ($100K-$119K)'
    ELSE 'Junior (<$100K)'
  END as salary_band,
  COUNT(*) as employee_count,
  AVG(CASE WHEN lastPromoted IS NOT NULL
      THEN DATE_DIFF('year', lastPromoted, '2025-01-01') END) as avg_years_since_promotion,
  COUNT(CASE WHEN lastPromoted IS NULL THEN 1 END) as never_promoted,
  ROUND(100.0 * COUNT(CASE WHEN lastPromoted IS NULL THEN 1 END) / COUNT(*), 2) as never_promoted_pct
FROM employees
GROUP BY salary_band
ORDER BY AVG(salary) DESC;
```

### High-Potential Candidates for Next Level
```sql
WITH promotion_analysis AS (
  SELECT employeeName, department, position, salary, location,
         EXTRACT(YEAR FROM '2025-01-01') - EXTRACT(YEAR FROM startDate) as tenure_years,
         CASE
           WHEN lastPromoted IS NULL THEN DATE_DIFF('year', startDate, '2025-01-01')
           ELSE DATE_DIFF('year', lastPromoted, '2025-01-01')
         END as years_since_promotion,
         CASE
           WHEN position LIKE '%Software Engineer%' AND NOT position LIKE 'Senior%' THEN 'Senior Software Engineer'
           WHEN position = 'Senior Software Engineer' THEN 'Lead Software Engineer'
           WHEN position = 'Lead Software Engineer' THEN 'Principal Software Engineer'
           WHEN position LIKE '%Analyst%' AND NOT position LIKE 'Senior%' THEN 'Senior ' || position
           WHEN position LIKE '%Specialist%' AND NOT position LIKE 'Senior%' THEN 'Senior ' || position
           WHEN position LIKE '%Coordinator%' THEN REPLACE(position, 'Coordinator', 'Manager')
           WHEN position LIKE '%Manager%' AND NOT position LIKE 'Senior%' THEN 'Senior ' || position
           ELSE 'Director Level'
         END as suggested_next_position
  FROM employees
)
SELECT employeeName, department, position, suggested_next_position, salary, location,
       tenure_years, years_since_promotion,
       CASE
         WHEN years_since_promotion >= 3 AND tenure_years >= 2 THEN 'Ready for Promotion'
         WHEN years_since_promotion >= 2 AND tenure_years >= 3 THEN 'Consider for Promotion'
         WHEN years_since_promotion >= 2 THEN 'Monitor Performance'
         ELSE 'Recently Advanced'
       END as recommendation
FROM promotion_analysis
WHERE years_since_promotion >= 2
ORDER BY years_since_promotion DESC, tenure_years DESC;
```

### Engineering Career Ladder Progression
```sql
SELECT
  position,
  COUNT(*) as current_count,
  AVG(salary) as avg_salary,
  AVG(EXTRACT(YEAR FROM '2025-01-01') - EXTRACT(YEAR FROM startDate)) as avg_tenure,
  AVG(CASE WHEN lastPromoted IS NOT NULL
      THEN DATE_DIFF('year', lastPromoted, '2025-01-01') END) as avg_years_since_promotion,
  COUNT(CASE WHEN lastPromoted IS NULL AND DATE_DIFF('year', startDate, '2025-01-01') >= 2
       THEN 1 END) as promotion_candidates
FROM employees
WHERE department = 'Engineering'
GROUP BY position
ORDER BY avg_salary;
```

## Usage Examples

Test these queries with the MCP DuckDB server:

```javascript
// Example: Get 15-year anniversary employees
await client.callTool({
  name: 'dbQueryTool',
  arguments: {
    sql: `SELECT employeeName, position, salary,
             EXTRACT(YEAR FROM '2025-01-01') - EXTRACT(YEAR FROM startDate) as tenure
          FROM employees
          WHERE EXTRACT(YEAR FROM startDate) <= 2010
          ORDER BY salary DESC`
  }
});
```

All queries support the streaming functionality with 1-second delays between 5-row chunks!