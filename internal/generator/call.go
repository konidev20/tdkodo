package generator

import (
	"fmt"
	"time"

	"github.com/brianvoe/gofakeit"
)

type Call struct {
}

var _ Generator = Call{}

func (c Call) CSVHeaders() string {
	return "call_id,employee_id,call_date,call_duration,customer_name,customer_phone,call_outcome,customer_feedback"
}

func (c Call) Table() string {
	return "calls"
}

func (c Call) CSVColumnMapping() string {
	return "(call_id INTEGER, employee_id INTEGER, call_date, call_duration INTEGER, customer_name, customer_phone, call_outcome CHAR(32000), customer_feedback CHAR(32000))"
}

func (c Call) FakeRecord() string {
	// call_id,employee_id,call_date,call_duration,customer_name,customer_phone,call_outcome,customer_feedback
	return fmt.Sprintf("%d,%d,%s,%d,%s,%s,%s,%s", gofakeit.Number(1, 1000000), gofakeit.Number(1, 1000000), gofakeit.DateRange(time.Date(2020, 1, 0, 0, 0, 0, 0, time.UTC), time.Now()).String(), gofakeit.Number(1, 1000000), gofakeit.Name(), gofakeit.Phone(), gofakeit.Paragraph(10, 10, 10, ""), gofakeit.Paragraph(10, 10, 10, ""))
}
