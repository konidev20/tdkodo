package models

import "time"

type Employee struct {
	ID         int       `json:"id"`
	FirstName  string    `json:"first_name" fake:"{firstname}"`
	LastName   string    `json:"last_name" fake:"{lastname}"`
	Email      string    `json:"email" fake:"{email}"`
	Phone      string    `json:"phone"`
	HireDate   time.Time `json:"hire_date" `
	JobTitle   string    `json:"job_title"`
	Department string    `json:"department"`
}

type Call struct {
	ID            int       `json:"id"`
	EmployeeID    int       `json:"employee_id"`
	CallDate      time.Time `json:"call_date" `
	CallDuration  int       `json:"call_duration"`
	CustomerName  string    `json:"customer_name"`
	CustomerPhone string    `json:"customer_phone"`
	CallOutcome   string    `json:"call_outcome" fake:"{paragraph:}"`
}

type Contract struct {
	ID           int       `json:"id"`
	EmployeeID   int       `json:"employee_id"`
	StartDate    time.Time `json:"start_date" `
	EndDate      time.Time `json:"end_date" `
	ContractType string    `json:"contract_type"`
	Salary       float64   `json:"salary"`
}

type Payroll struct {
	ID             int       `json:"id"`
	EmployeeID     int       `json:"employee_id"`
	PayPeriodStart time.Time `json:"pay_period_start" `
	PayPeriodEnd   time.Time `json:"pay_period_end" `
	BaseSalary     float64   `json:"base_salary"`
	Bonus          float64   `json:"bonus"`
	Deductions     float64   `json:"deductions"`
	NetPay         float64   `json:"net_pay"`
}
