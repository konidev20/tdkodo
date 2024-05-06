package generator

import (
	"fmt"

	"github.com/brianvoe/gofakeit"
)

type User struct {
	FirstName   string `db:"first_name"`
	LastName    string `db:"last_name"`
	Email       string `db:"email"`
	Age         int    `db:"age"`
	Description string `db:"description"`
}

var _ Generator = User{}

func (u User) CSVHeaders() string {
	return "first_name,last_name,email,age,description"
}

func (u User) CSVColumnMapping() string {
	return "(first_name CHAR(100), last_name CHAR(100), email CHAR(100), age INTEGER, description CHAR(32000))"
}

func (u User) Table() string {
	return "users"
}

func (u User) FakeRecord() string {
	return fmt.Sprintf("%s,%s,%s,%d,%s", gofakeit.FirstName(), gofakeit.LastName(), gofakeit.Email(), gofakeit.Number(18, 75), gofakeit.Sentence(10))
}
