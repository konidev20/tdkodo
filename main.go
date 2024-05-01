/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
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
	n := flag.Int("batches", 2000, "--batches=3000")
	batchSize := flag.Int("batchSize", 1000, "--batchSize=1000")
	cycles := flag.Int("cycles", 20, "--cycles=20")

	flag.Parse()

	wg := errgroup.Group{}

	type controlFile struct {
		runID           string
		controlFileName string
	}

	controlFiles := make(chan controlFile, *cycles)

	start := time.Now()

	wg.Go(func() error {
		for controlFile := range controlFiles {
			log.Printf("sqlldr triggered for runID: %s controlFileName: %s", controlFile.runID, controlFile.controlFileName)
			err := runSQLLoader(controlFile.controlFileName)
			if err != nil {
				log.Printf("failed to run sqlldr for runID: %s controlFileName: %s : %s", controlFile.runID, controlFile.controlFileName, err.Error())
				return err
			}
			log.Printf("sqlldr completed for runID: %s controlFileName: %s", controlFile.runID, controlFile.controlFileName)
		}
		return nil
	})

	for _ = range *cycles {
		runID := gofakeit.UUID()

		controlFileName, err := generateFakeCallRecordsToCSV(runID, *n, *batchSize)
		if err != nil {
			log.Printf("failed to generate data for runID: %s : %s", runID, err.Error())
			return
		}

		controlFiles <- controlFile{runID: runID, controlFileName: controlFileName}
	}

	close(controlFiles)

	err := wg.Wait()
	if err != nil {
		log.Printf("error: %s", err.Error())
		return
	}
	elapsed := time.Since(start)
	log.Default().Printf("total time taken: %s", elapsed)
}

func runSQLLoader(controlFileName string) error {
	cmd := exec.Command("sqlldr", "userid=\"/ as sysdba\"", "control="+controlFileName, "direct=true", "log="+controlFileName+".log", "parallel=true")
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to run sqlldr: %s", err.Error())
	}
	return nil
}

func generateFakeCallRecordsToCSV(runID string, n, batchSize int) (string, error) {
	dataGenStart := time.Now()
	wg := errgroup.Group{}

	var processed atomic.Int64

	for i := range n {
		wg.Go(func() error {
			err := createFakeCallRecordsBatchToCSV(runID, i, batchSize)
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
		return "", err
	}

	controlFileName := fmt.Sprintf("calls-control-%s.ctl", runID)

	err = createCallsControlFile(runID, controlFileName, n)
	if err != nil {
		return "", err
	}

	dataGenStartElapsed := time.Since(dataGenStart)
	log.Printf("generated %d csv files for runID: %s controlFileName: %s time elapsed: %s", processed.Load(), runID, controlFileName, dataGenStartElapsed)
	return controlFileName, nil
}

func createCallsControlFile(runID, controlFileName string, n int) error {
	controlFile, err := os.Create(controlFileName)
	if err != nil {
		errMsg := fmt.Sprintf("failed to create and open file %s: %s", controlFileName, err.Error())
		log.Print(errMsg)
	}

	_, err = fmt.Fprintln(controlFile, "LOAD DATA")
	if err != nil {
		errMsg := fmt.Sprintf("failed to write LOAD DATA to file %s: %s", controlFileName, err.Error())
		log.Print(errMsg)
	}

	for i := range n {
		_, err = fmt.Fprintln(controlFile, fmt.Sprintf("INFILE 'calls-%s-batchNumber-%d.csv'", runID, i))
		if err != nil {
			errMsg := fmt.Sprintf("failed to write INFILE to file %s: %s", controlFileName, err.Error())
			log.Print(errMsg)
		}
	}

	_, err = fmt.Fprintln(controlFile, "APPEND")
	if err != nil {
		errMsg := fmt.Sprintf("failed to write APPEND to file %s: %s", controlFileName, err.Error())
		log.Print(errMsg)
	}

	_, err = fmt.Fprintln(controlFile, "INTO TABLE sys.calls")
	if err != nil {
		errMsg := fmt.Sprintf("failed to write INTO TABLE to file %s: %s", controlFileName, err.Error())
		log.Print(errMsg)
	}

	_, err = fmt.Fprintln(controlFile, "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'")
	if err != nil {
		errMsg := fmt.Sprintf("failed to write FIELDS TERMINATED BY ',' to file %s: %s", controlFileName, err.Error())
		log.Print(errMsg)
	}

	_, err = fmt.Fprintln(controlFile, "(call_id INTEGER, employee_id INTEGER, call_date, call_duration INTEGER, customer_name, customer_phone, call_outcome CHAR(32000), customer_feedback CHAR(32000))")
	if err != nil {
		errMsg := fmt.Sprintf("failed to write fields to file %s: %s", controlFileName, err.Error())
		log.Print(errMsg)
	}
	return err
}

func createFakeCallRecordsBatchToCSV(runID string, batchNumber, batchSize int) error {
	producerWg := errgroup.Group{}

	calls := make(chan models.Call, batchSize)

	csvFileName := fmt.Sprintf("calls-%s-batchNumber-%d.csv", runID, batchNumber)

	csvFile, err := os.Create(csvFileName)
	if err != nil {
		errMsg := fmt.Sprintf("failed to create and open file %s: %s", csvFileName, err.Error())
		log.Print(errMsg)
		return fmt.Errorf(errMsg)
	}

	headers := fmt.Sprintf("call_id,employee_id,call_date,call_duration,customer_name,customer_phone,call_outcome,customer_feedback")

	_, err = fmt.Fprintln(csvFile, headers)
	if err != nil {
		errMsg := fmt.Sprintf("failed to write insert query to file %s: %s", csvFileName, err.Error())
		log.Print(errMsg)
		return fmt.Errorf(errMsg)
	}

	for _ = range batchSize {
		producerWg.Go(func() error {
			call := createFakeCallRecordSync()
			line := fmt.Sprintf("%d,%d,%s,%d,%s,%s,%s,%s", call.ID, call.EmployeeID, call.CallDate, call.CallDuration, call.CustomerName, call.CustomerPhone, call.CallOutcome, call.CustomerFeedback)
			_, err = fmt.Fprintln(csvFile, line)
			if err != nil {
				errMsg := fmt.Sprintf("failed to write insert query to file %s: %s", csvFileName, err.Error())
				log.Print(errMsg)
				return fmt.Errorf(errMsg)
			}
			return nil
		})
	}

	producerWg.Wait()

	close(calls)

	csvFile.Sync()

	//log.Printf("runID:%s batchNumber: %d batchSize: %d", runID, batchNumber, batchSize)

	return err
}

// creates a fake call struct and writes it into the out channel
func createFakeCallRecordSync() models.Call {
	var call models.Call
	call.ID = gofakeit.Number(1, 1000000)
	call.EmployeeID = gofakeit.Number(1, 1000000)
	call.CallDuration = gofakeit.Number(1, 1000000)
	call.CallOutcome = gofakeit.Paragraph(10, 10, 10, "")
	call.CustomerFeedback = gofakeit.Paragraph(10, 10, 10, "")
	call.CallDate = gofakeit.DateRange(time.Date(2020, 1, 0, 0, 0, 0, 0, time.UTC), time.Now()).String()
	call.CustomerName = gofakeit.Name()
	call.CustomerPhone = gofakeit.Phone()
	return call
}
