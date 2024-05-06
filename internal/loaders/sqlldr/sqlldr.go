package sqlldr

import (
	"fmt"
	"log"
	"os/exec"
)

func Load(runID, controlFileName string) error {
	log.Printf("sqlldr triggered for runID: %s controlFileName: %s", runID, controlFileName)
	cmd := exec.Command("sqlldr", "userid=\"/ as sysdba\"", "control="+controlFileName, "direct=true", "log="+controlFileName+".log", "parallel=true")
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to run sqlldr: %s", err.Error())
	}
	log.Printf("sqlldr completed for runID: %s controlFileName: %s", runID, controlFileName)
	return nil
}
