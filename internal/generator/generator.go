/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package generator

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/konidev20/tdkodo/internal/loaders/sqlldr"
	"golang.org/x/sync/errgroup"
)

type controlFile struct {
	table           string
	runID           string
	controlFileName string
}

func Run(n, batchSize, cycles int) {
	wg := errgroup.Group{}

	controlFiles := make(chan controlFile, cycles)
	loadedControlFiles := make(chan controlFile, cycles)

	start := time.Now()

	cleanupWg := errgroup.Group{}
	cleanupWg.Go(func() error {
		// CLEANER: will delete data which is loaded into the database
		for controlFile := range loadedControlFiles {
			err := os.RemoveAll(fmt.Sprintf("./%s/%s", controlFile.table, controlFile.runID))
			if err != nil {
				log.Printf("failed to delete directory for runID: %s : %s", controlFile.runID, err.Error())
				return err
			}
		}
		return nil
	})

	wg.Go(func() error {
		// LOADER: will load any given control file, not dependent on the table name
		for controlFile := range controlFiles {
			err := sqlldr.Load(controlFile.runID, controlFile.controlFileName)
			if err != nil {
				log.Printf("failed to run sqlldr for runID: %s controlFileName: %s : %s", controlFile.runID, controlFile.controlFileName, err.Error())
				return err
			}
			loadedControlFiles <- controlFile
		}
		return nil
	})

	for range cycles {
		runID := gofakeit.UUID()

		var call Call

		table := call.Table()
		headers := call.CSVHeaders()
		columns := call.CSVColumns()

		controlFileName, err := GenerateForTable(runID, table, headers, columns, n, batchSize, call.FakeRecord)
		if err != nil {
			log.Printf("failed to generate data for runID: %s : %s", runID, err.Error())
			return
		}

		controlFiles <- controlFile{table: table, runID: runID, controlFileName: controlFileName}
	}

	close(controlFiles)

	err := wg.Wait()
	if err != nil {
		log.Printf("error: %s", err.Error())
		return
	}

	close(loadedControlFiles)

	err = cleanupWg.Wait()
	if err != nil {
		log.Printf("error: %s", err.Error())
		return
	}

	elapsed := time.Since(start)
	log.Default().Printf("total time taken: %s", elapsed)
}

func GenerateForTable(runID, table, headers, columns string, n, batchSize int, lineFn func() string) (string, error) {
	dataGenStart := time.Now()
	wg := errgroup.Group{}

	var processed atomic.Int64

	for i := range n {
		wg.Go(func() error {
			err := generateCSVDataFile(runID, table, headers, i, batchSize, lineFn)
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

	controlFileName := fmt.Sprintf("./%s/control-%s.ctl", table, runID)

	err = createControlFile(runID, table, columns, controlFileName, n)
	if err != nil {
		return "", err
	}

	dataGenStartElapsed := time.Since(dataGenStart)
	log.Printf("generated %d csv files for table: %s runID: %s controlFileName: %s time elapsed: %s", processed.Load(), table, runID, controlFileName, dataGenStartElapsed)
	return controlFileName, nil
}

func createControlFile(runID, table, columns, controlFileName string, n int) error {
	err := os.MkdirAll(table, 0755)
	if err != nil {
		errMsg := fmt.Sprintf("failed to create directory %s: %s", table, err.Error())
		log.Print(errMsg)
		return fmt.Errorf(errMsg)
	}

	controlFile, err := os.Create(controlFileName)
	if err != nil {
		errMsg := fmt.Sprintf("failed to create and open file %s: %s", controlFileName, err.Error())
		log.Print(errMsg)
		return fmt.Errorf(errMsg)
	}

	_, err = fmt.Fprintln(controlFile, "LOAD DATA")
	if err != nil {
		errMsg := fmt.Sprintf("failed to write LOAD DATA to file %s: %s", controlFileName, err.Error())
		log.Print(errMsg)
		return fmt.Errorf(errMsg)
	}

	for i := range n {
		_, err = fmt.Fprintf(controlFile, "\nINFILE './%s/%s/batchNumber-%d.csv'", table, runID, i)
		if err != nil {
			errMsg := fmt.Sprintf("failed to write INFILE to file %s: %s", controlFileName, err.Error())
			log.Print(errMsg)
			return fmt.Errorf(errMsg)
		}
	}

	_, err = fmt.Fprintln(controlFile, "\nAPPEND")
	if err != nil {
		errMsg := fmt.Sprintf("failed to write APPEND to file %s: %s", controlFileName, err.Error())
		log.Print(errMsg)
		return fmt.Errorf(errMsg)
	}

	_, err = fmt.Fprintf(controlFile, "INTO TABLE %s\n", table)
	if err != nil {
		errMsg := fmt.Sprintf("failed to write INTO TABLE to file %s: %s", controlFileName, err.Error())
		log.Print(errMsg)
		return fmt.Errorf(errMsg)
	}

	_, err = fmt.Fprintln(controlFile, "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'")
	if err != nil {
		errMsg := fmt.Sprintf("failed to write FIELDS TERMINATED BY ',' to file %s: %s", controlFileName, err.Error())
		log.Print(errMsg)
		return fmt.Errorf(errMsg)
	}

	_, err = fmt.Fprintln(controlFile, columns)
	if err != nil {
		errMsg := fmt.Sprintf("failed to write fields to file %s: %s", controlFileName, err.Error())
		log.Print(errMsg)
		return fmt.Errorf(errMsg)
	}
	return err
}

func generateCSVDataFile(runID, table, headers string, batchNumber, batchSize int, lineFn func() string) error {
	producerWg := errgroup.Group{}

	err := os.MkdirAll(filepath.Join(table, runID), 0755)
	if err != nil {
		errMsg := fmt.Sprintf("failed to create directory %s: %s", table, err.Error())
		log.Print(errMsg)
		return fmt.Errorf(errMsg)
	}

	csvFileName := fmt.Sprintf("./%s/%s/batchNumber-%d.csv", table, runID, batchNumber)

	csvFile, err := os.Create(csvFileName)
	if err != nil {
		errMsg := fmt.Sprintf("failed to create and open file %s: %s", csvFileName, err.Error())
		log.Print(errMsg)
		return fmt.Errorf(errMsg)
	}

	_, err = fmt.Fprintln(csvFile, headers)
	if err != nil {
		errMsg := fmt.Sprintf("failed to write record to file %s: %s", csvFileName, err.Error())
		log.Print(errMsg)
		return fmt.Errorf(errMsg)
	}

	for range batchSize {
		producerWg.Go(func() error {
			line := lineFn()
			_, err = fmt.Fprintln(csvFile, line)
			if err != nil {
				errMsg := fmt.Sprintf("failed to write record to file %s: %s", csvFileName, err.Error())
				log.Print(errMsg)
				return fmt.Errorf(errMsg)
			}
			return nil
		})
	}

	producerWg.Wait()

	csvFile.Sync()

	return err
}

type Call struct {
}

func (c Call) CSVHeaders() string {
	return "call_id,employee_id,call_date,call_duration,customer_name,customer_phone,call_outcome,customer_feedback"
}

func (c Call) Table() string {
	return "calls"
}

func (c Call) CSVColumns() string {
	return "(call_id INTEGER, employee_id INTEGER, call_date, call_duration INTEGER, customer_name, customer_phone, call_outcome CHAR(32000), customer_feedback CHAR(32000))"
}

func (c Call) FakeRecord() string {
	// call_id,employee_id,call_date,call_duration,customer_name,customer_phone,call_outcome,customer_feedback
	return fmt.Sprintf("%d,%d,%s,%d,%s,%s,%s,%s", gofakeit.Number(1, 1000000), gofakeit.Number(1, 1000000), gofakeit.DateRange(time.Date(2020, 1, 0, 0, 0, 0, 0, time.UTC), time.Now()).String(), gofakeit.Number(1, 1000000), gofakeit.Name(), gofakeit.Phone(), gofakeit.Paragraph(10, 10, 10, ""), gofakeit.Paragraph(10, 10, 10, ""))
}
