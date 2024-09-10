/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
		"docker",
		"compose",
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
		"docker",
		"compose",
		"-p", c.project,
		"-f", c.filename,
		"down", "-v").CombinedOutput()
	ctx.Log(string(out))

	return err
}

func Start(project, filename string, services ...string) flow.Runnable {
	return New(project, filename).Start(services...)
}

func (c Compose) Start(services ...string) flow.Runnable {
	return func(ctx flow.Context) error {
		args := []string{
			"compose",
			"-p", c.project,
			"-f", c.filename,
			"start",
		}
		args = append(args, services...)
		out, err := exec.Command("docker", args...).CombinedOutput()
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
			"compose",
			"-p", c.project,
			"-f", c.filename,
			"stop",
		}
		args = append(args, services...)
		out, err := exec.Command("docker", args...).CombinedOutput()
		ctx.Log(string(out))
		return err
	}
}
