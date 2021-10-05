package terraform

import (
	"os/exec"
	"github.com/dapr/components-contrib/tests/poc/pubsub/kafka/pkg/flow"
)

type Terraform struct {
	project  string
}

func Run(project) (string, flow.Runnable, flow.Runnable) {
	return New(project).ToStep()
}

func New(project) Terraform {
	return Terraform{
		project:  project,
	}
}

func (t Terraform) AppID() string {
	return t.project
}

func (t Terraform) ToStep() (string, flow.Runnable, flow.Runnable, flow.Runnable) {
	return t.project, t.init, t.apply, t.destroy
}

func init(project) flow.Runnable {
	return New(project).init
}

func (t terrafom) init(ctx flow.Context) error{
		out, err := exec.Command("terraform", "init").CombinedOutput()
		ctx.Log(string(out))
		return err
}

func show(project) flow.Runnable {
	return New(project).show
}

func (t terrafom) show(ctx flow.Context) error{
	out, err := exec.Command("terraform", "show").CombinedOutput()
	ctx.Log(string(out))
	return err
}

func apply(project) flow.Runnable {
	return New(project).apply
}

func (t terrafom) apply(ctx flow.Context) error{
	out, err := exec.Command("terraform", "apply").CombinedOutput()
	ctx.Log(string(out))
	return err
}

func destroy(project) flow.Runnable {
	return New(project).destroy
}

func (t terrafom) destroy(ctx flow.Context) error{
	out, err := exec.Command("terraform", "destroy").CombinedOutput()
	ctx.Log(string(out))
	return err
}
