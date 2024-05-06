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

	"golang.org/x/sync/errgroup"
)

type Generator interface {
	Table() string
	CSVHeaders() string
	CSVColumnMapping() string
	FakeRecord() string
}

func Generate(runID string, n, batchSize int, generator Generator) (string, error) {
	table := generator.Table()
	headers := generator.CSVHeaders()
	columns := generator.CSVColumnMapping()

	dataGenStart := time.Now()
	wg := errgroup.Group{}

	var processed atomic.Int64

	for i := range n {
		wg.Go(func() error {
			err := generateCSVDataFile(runID, table, headers, i, batchSize, generator.FakeRecord)
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
