/*
Copyright 2022 The Dapr Authors
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

package terraform

import (
	"os/exec"

	"github.com/dapr/components-contrib/tests/certification/flow"
)

type Terraform struct {
	project  string
	filename string
}

func Run(project, filename string) (string, flow.Runnable, flow.Runnable) {
	return New(project, filename).ToStep()
}

func New(project, filename string) Terraform {
	return Terraform{
		project:  project,
		filename: filename,
	}
}

func (t Terraform) ToStep() (string, flow.Runnable, flow.Runnable) {
	return t.project, t.init, t.apply
}

func (t Terraform) AppID() string {
	return t.project
}

func initiate(project, filename string) flow.Runnable {
	return New(project, filename).init
}

func (t Terraform) init(ctx flow.Context) error {
	out, err := exec.Command("terraform", "init").CombinedOutput()
	ctx.Log(string(out))
	return err
}

func apply(project, filename string) flow.Runnable {
	return New(project, filename).apply
}

func (t Terraform) apply(ctx flow.Context) error {
	out, err := exec.Command("terraform", "apply").CombinedOutput()
	ctx.Log(string(out))
	return err
}

func show(project, filename string) flow.Runnable {
	return New(project, filename).show
}

func (t Terraform) show(ctx flow.Context) error {
	out, err := exec.Command("terraform", "show").CombinedOutput()
	ctx.Log(string(out))
	return err
}

func destroy(project, filename string) flow.Runnable {
	return New(project, filename).destroy
}

func (t Terraform) destroy(ctx flow.Context) error {
	out, err := exec.Command("terraform", "destroy").CombinedOutput()
	ctx.Log(string(out))
	return err
}
