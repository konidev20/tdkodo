package cmd

import (
	"log"

	"github.com/konidev20/tdkodo/internal/generator"
	"github.com/spf13/cobra"
)

type RootOptions struct {
	Cycles    int
	Batches   int
	BatchSize int
}

var ropts RootOptions

var rootCmd = &cobra.Command{
	Use:   "tdkodo [flags]",
	Short: "Populate an Oracle Database with test data to test backup and recovery scripts.",
	Run: func(cmd *cobra.Command, args []string) {
		generator.Run(ropts.Batches, ropts.BatchSize, ropts.Cycles)
	},
}

func init() {
	rootCmd.Flags().IntVarP(&ropts.Cycles, "cycles", "c", 20, "Number of cycles to run")
	rootCmd.Flags().IntVarP(&ropts.Batches, "batches", "b", 2000, "Number of batches to run")
	rootCmd.Flags().IntVarP(&ropts.BatchSize, "batchSize", "s", 1000, "Number of records per batch")
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		log.Fatalf("Error executing command: %s", err)
	}
}
