// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package dockercompose

import (
	"os/exec"

	"github.com/dapr/components-contrib/tests/certification/flow"
)

type Compose struct {
	project  string
	filename string
}

func Run(project, filename string) (string, flow.Runnable, flow.Runnable) {
	return New(project, filename).ToStep()
}

func New(project, filename string) Compose {
	return Compose{
		project:  project,
		filename: filename,
	}
}

func (c Compose) AppID() string {
	return c.project
}

func (c Compose) ToStep() (string, flow.Runnable, flow.Runnable) {
	return c.project, c.Up, c.Down
}

func Up(project, filename string) flow.Runnable {
	return New(project, filename).Up
}

func (c Compose) Up(ctx flow.Context) error {
	out, err := exec.Command(
		"docker-compose",
		"-p", c.project,
		"-f", c.filename,
		"up", "-d",
		"--remove-orphans").CombinedOutput()
	ctx.Log(string(out))

	return err
}

func Down(project, filename string) flow.Runnable {
	return New(project, filename).Down
}

func (c Compose) Down(ctx flow.Context) error {
	out, err := exec.Command(
		"docker-compose",
		"-p", c.project,
		"-f", c.filename,
		"down").CombinedOutput()
	ctx.Log(string(out))

	return err
}

func Start(project, filename string, services ...string) flow.Runnable {
	return New(project, filename).Start(services...)
}

func (c Compose) Start(services ...string) flow.Runnable {
	return func(ctx flow.Context) error {
		args := []string{
			"-p", c.project,
			"-f", c.filename,
			"start",
		}
		args = append(args, services...)
		out, err := exec.Command("docker-compose", args...).CombinedOutput()
		ctx.Log(string(out))
		return err
	}
}

func Stop(project, filename string, services ...string) flow.Runnable {
	return New(project, filename).Stop(services...)
}

func (c Compose) Stop(services ...string) flow.Runnable {
	return func(ctx flow.Context) error {
		args := []string{
			"-p", c.project,
			"-f", c.filename,
			"stop",
		}
		args = append(args, services...)
		out, err := exec.Command("docker-compose", args...).CombinedOutput()
		ctx.Log(string(out))
		return err
	}
}
