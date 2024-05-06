package cmd

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/konidev20/tdkodo/internal/generator"
	"github.com/konidev20/tdkodo/internal/loaders/sqlldr"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

type RootOptions struct {
	Cycles    int
	Batches   int
	BatchSize int
	Table     string
	KeepData  bool
}

var ropts RootOptions

var rootCmd = &cobra.Command{
	Use:   "tdkodo [flags]",
	Short: "Populate an Oracle Database with test data to test backup and recovery scripts.",
	Run: func(cmd *cobra.Command, args []string) {
		Run(ropts.Batches, ropts.BatchSize, ropts.Cycles, ropts.Table)
	},
}

func init() {
	rootCmd.Flags().IntVarP(&ropts.Cycles, "cycles", "c", 20, "Number of cycles to run")
	rootCmd.Flags().IntVarP(&ropts.Batches, "batches", "b", 2000, "Number of batches to run")
	rootCmd.Flags().IntVarP(&ropts.BatchSize, "batchSize", "s", 1000, "Number of records per batch")
	rootCmd.Flags().StringVarP(&ropts.Table, "table", "t", "calls", "Table to populate")
	rootCmd.Flags().BoolVarP(&ropts.KeepData, "keepData", "k", false, "Keep data after run")
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		log.Fatalf("Error executing command: %s", err)
	}
}

type controlFile struct {
	table           string
	runID           string
	controlFileName string
}

func Run(n, batchSize, cycles int, table string) {
	wg := errgroup.Group{}

	controlFiles := make(chan controlFile, cycles)
	loadedControlFiles := make(chan controlFile, cycles)

	start := time.Now()

	cleanupWg := errgroup.Group{}
	if !ropts.KeepData {
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
	}

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

	g := getGenerator(table)

	for range cycles {
		runID := gofakeit.UUID()
		controlFileName, err := generator.Generate(runID, n, batchSize, g)
		if err != nil {
			log.Printf("failed to generate data for runID: %s : %s", runID, err.Error())
			return
		}

		controlFiles <- controlFile{table: g.Table(), runID: runID, controlFileName: controlFileName}
	}

	close(controlFiles)

	err := wg.Wait()
	if err != nil {
		log.Printf("error: %s", err.Error())
	}

	close(loadedControlFiles)

	err = cleanupWg.Wait()
	if err != nil {
		log.Printf("error: %s", err.Error())
	}

	elapsed := time.Since(start)
	log.Default().Printf("total time taken: %s", elapsed)
}

func getGenerator(table string) generator.Generator {
	switch table {
	case "calls":
		return generator.Call{}
	case "users":
		return generator.User{}
	default:
		return generator.Call{}
	}
}
