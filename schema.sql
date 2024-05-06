CREATE TABLE calls (
  call_id NUMBER,
  employee_id NUMBER,
  call_date VARCHAR(1000),
  call_duration NUMBER,
  customer_name VARCHAR2(100),
  customer_phone VARCHAR2(20),
  call_outcome CLOB, 
  customer_feedback CLOB
  );