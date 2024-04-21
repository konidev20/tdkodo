CREATE TABLE employees (
  id SERIAL PRIMARY KEY,
  first_name VARCHAR(50),
  last_name VARCHAR(50),
  email VARCHAR(100),
  phone VARCHAR(20),
  hire_date DATE,
  job_title VARCHAR(50),
  department VARCHAR(50)
);

CREATE TABLE calls (
  id SERIAL PRIMARY KEY,
  employee_id INTEGER,
  call_date DATE,
  call_duration INTEGER,
  customer_name VARCHAR(100),
  customer_phone VARCHAR(20),
  call_outcome VARCHAR(100)
);

CREATE TABLE contracts (
  id SERIAL PRIMARY KEY,
  employee_id INTEGER,
  start_date DATE,
  end_date DATE,
  contract_type VARCHAR(50),
  salary DECIMAL(10, 2)
);

CREATE TABLE payroll (
  id SERIAL PRIMARY KEY,
  employee_id INTEGER,
  pay_period_start DATE,
  pay_period_end DATE,
  base_salary DECIMAL(10, 2),
  bonus DECIMAL(10, 2),
  deductions DECIMAL(10, 2),
  net_pay DECIMAL(10, 2)
);