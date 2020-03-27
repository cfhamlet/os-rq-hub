package main

import (
	cmd "github.com/cfhamlet/os-rq-hub/cmd/rq-hub/command"
	"github.com/cfhamlet/os-rq-pod/pkg/command"
)

func main() {
	command.Execute(cmd.Root)
}
