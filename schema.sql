CREATE TABLE calls (
  id SERIAL PRIMARY KEY,
  employee_id INTEGER,
  call_date DATE,
  call_duration INTEGER,
  customer_name VARCHAR(100),
  customer_phone VARCHAR(20),
  call_outcome VARCHAR(100)
);
