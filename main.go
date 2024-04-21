/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"

	"github.com/brianvoe/gofakeit"
	"github.com/konidev20/tdkodo/models"
	"golang.org/x/sync/errgroup"
)

var runID string

func init() {
	runID = gofakeit.UUID()
}

func main() {

	n := 200
	batchSize := 1000

	generateFakeCalls(n, batchSize)
}

func generateFakeCalls(n, batchSize int) {
	wg := errgroup.Group{}

	var processed atomic.Int64

	for i := range n {
		wg.Go(func() error {
			err := createFakeCallsBatch(i, batchSize)
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

func createFakeCallsBatch(batchNumber, batchSize int) error {
	producerWg := errgroup.Group{}
	consumerWg := errgroup.Group{}

	var placeholders []string
	// var values []interface{}
	var values []string

	calls := make(chan models.Call, batchSize)

	for _ = range batchSize {
		producerWg.Go(func() error {
			createFakeCall(calls)
			return nil
		})
	}

	consumerWg.Go(func() error {
		for call := range calls {
			placeholders = append(placeholders, "(?, ?, ?, ?, ?, ?)")
			// values = append(values, call.EmployeeID, call.CallDate, call.CallDuration, call.CustomerName, call.CustomerPhone, call.CallOutcome)
			values = append(values, string(call.EmployeeID), call.CallDate.String(), string(call.CallDuration), call.CustomerName, call.CustomerPhone, call.CallOutcome)
		}

		return nil
	})

	producerWg.Wait()

	close(calls)

	consumerWg.Wait()

	//query := "INSERT INTO calls (employee_id, call_date, call_duration, customer_name, customer_phone, call_outcome) VALUES " + strings.Join(placeholders, ", ")
	query := "INSERT INTO calls (employee_id, call_date, call_duration, customer_name, customer_phone, call_outcome) VALUES " + strings.Join(values, ", ")

	queryFileName := fmt.Sprintf("calls-%s-batchNumber-%d", runID, batchNumber)

	queryFile, err := os.Create(queryFileName)
	if err != nil {
		errMsg := fmt.Sprintf("failed to create and open file %s: %s", queryFileName, err.Error())
		log.Printf(errMsg)
		return fmt.Errorf(errMsg)
	}

	_, err = fmt.Fprintln(queryFile, query)
	if err != nil {
		errMsg := fmt.Sprintf("failed to write insert query to file %s: %s", queryFileName, err.Error())
		log.Printf(errMsg)
		return fmt.Errorf(errMsg)
	}

	log.Println("runID:%s batchNumber: %d batchSize: %d", runID, batchNumber, batchSize)

	return err
}

// creates a fake call struct and writes it into the out channel
func createFakeCall(out chan<- models.Call) {
	var call models.Call
	gofakeit.Struct(&call)
	call.CallOutcome = gofakeit.Paragraph(20, 20, 20, ".")
	out <- call
}
