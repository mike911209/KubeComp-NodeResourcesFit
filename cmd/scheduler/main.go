package main

import (
	"log"
	noderesources "my-noderesourcesfit/pkg/plugins/noderesources"
	"os"

	"k8s.io/component-base/cli"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"
)

func main() {
	// Register custom plugins to the scheduler framework.
	log.Printf("my-noderesourcesfit starts!\n")
	command := app.NewSchedulerCommand(
		app.WithPlugin(noderesources.Name, noderesources.NewFit),
	)

	code := cli.Run(command)
	os.Exit(code)
}
