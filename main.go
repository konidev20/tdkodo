/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/konidev20/tdkodo/models"
	"golang.org/x/sync/errgroup"

	_ "github.com/godror/godror"
)

const (
	dbUser     = ""
	dbPassword = ""
	dbHost     = ""
	dbPort     = ""
	dbService  = ""
)

func main() {
	n := 3000
	batchSize := 100

	runID := gofakeit.UUID()
	generateFakeCallRecords(runID, n, batchSize)
}

func generateFakeCallRecords(runID string, n, batchSize int) {
	wg := errgroup.Group{}

	var processed atomic.Int64

	for i := range n {
		wg.Go(func() error {
			err := createFakeCallRecordsBatch(runID, i, batchSize)
			if err != nil {
				return err
			} else {
				processed.Add(int64(batchSize))
			}
			return nil
		})
	}

	err := wg.Wait()
	if err != nil {
		errMsg := fmt.Sprintf("errors: %s", err.Error())
		log.Printf(errMsg)
	}

	log.Printf("total number of rows: %d", processed.Load())
}

func createFakeCallRecordsBatch(runID string, batchNumber, batchSize int) error {
	producerWg := errgroup.Group{}
	consumerWg := errgroup.Group{}

	stringBuilder := strings.Builder{}

	stringBuilder.WriteString("INSERT ALL ")

	calls := make(chan models.Call, batchSize)

	for _ = range batchSize {
		producerWg.Go(func() error {
			createFakeCallRecord(calls)
			return nil
		})
	}

	consumerWg.Go(func() error {
		for call := range calls {
			stringBuilder.WriteString(fmt.Sprintf(" INTO sys.calls (call_id, employee_id, call_date, call_duration, customer_name, customer_phone, call_outcome, customer_feedback) VALUES (%d, %d, '%s', %d, '%s', '%s', '%s', '%s')", call.ID, call.EmployeeID, call.CallDate, call.CallDuration, call.CustomerName, call.CustomerPhone, call.CallOutcome, call.CustomerFeedback))
		}

		return nil
	})

	producerWg.Wait()

	close(calls)

	consumerWg.Wait()

	stringBuilder.WriteString(" SELECT * FROM DUAL")

	query := stringBuilder.String()

	queryFileName := fmt.Sprintf("calls-%s-batchNumber-%d", runID, batchNumber)

	err := writeQueryToFile(queryFileName, query, runID, batchNumber, batchSize)
	if err != nil {
		log.Print(err.Error())
	}

	// Open a connection to the Oracle database
	db, err := sql.Open("godror", fmt.Sprintf("%s/%s@%s:%s/%s as sysdba", dbUser, dbPassword, dbHost, dbPort, dbService))
	if err != nil {
		return fmt.Errorf("failed to open connection to Oracle database: %s", err.Error())
	}
	defer db.Close()

	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to insert records: %s", err.Error())
	}

	log.Printf("runID:%s batchNumber: %d batchSize: %d", runID, batchNumber, batchSize)

	return err
}

func writeQueryToFile(queryFileName string, query string, runID string, batchNumber int, batchSize int) error {
	queryFile, err := os.Create(queryFileName)
	if err != nil {
		errMsg := fmt.Sprintf("failed to create and open file %s: %s", queryFileName, err.Error())
		log.Print(errMsg)
		return fmt.Errorf(errMsg)
	}

	_, err = fmt.Fprintln(queryFile, query)
	if err != nil {
		errMsg := fmt.Sprintf("failed to write insert query to file %s: %s", queryFileName, err.Error())
		log.Print(errMsg)
		return fmt.Errorf(errMsg)
	}

	log.Printf("insert query written to file %s runID:%s batchNumber: %d batchSize: %d", queryFileName, runID, batchNumber, batchSize)
	return nil
}

// creates a fake call struct and writes it into the out channel
func createFakeCallRecord(out chan<- models.Call) {
	var call models.Call
	call.ID = gofakeit.Number(1, 1000000)
	call.EmployeeID = gofakeit.Number(1, 1000000)
	call.CallDuration = gofakeit.Number(1, 1000000)
	call.CallOutcome = gofakeit.Paragraph(10, 5, 10, "")
	call.CustomerFeedback = gofakeit.Paragraph(10, 5, 10, "")
	call.CallDate = gofakeit.DateRange(time.Date(2020, 1, 0, 0, 0, 0, 0, time.UTC), time.Now()).String()
	call.CustomerName = gofakeit.Name()
	call.CustomerPhone = gofakeit.Phone()
	out <- call
}
