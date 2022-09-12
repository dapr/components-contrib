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

package main

import (
	"context"
	"time"

	"github.com/zouyx/agollo/v3/component/log"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func main() {
	// Sleep for a bit so the docker container can spin up
	time.Sleep(30 * time.Second)
	TaskQueueString := "TestTaskQueue"

	// construct client here
	cOpt := client.Options{}
	cOpt.HostPort = "temporal:7233"
	cOpt.Identity = "TemporalTestClient"
	// Create the workflow client
	clientTwo, err := client.Dial(cOpt)
	if err != nil {
		log.Error("Unable to create client.")
		return
	}
	wOpt := worker.Options{}
	// Make default options for task q and worker options and workflow options
	w := worker.New(clientTwo, TaskQueueString, wOpt)

	// Register workflows and activities
	w.RegisterWorkflow(TestWorkflow)
	w.RegisterActivity(ExampleActivity)

	err = w.Start()
	if err != nil {
		log.Error("Unable to start worker.")
		return
	}
	w.Run(worker.InterruptCh())
}

func TestWorkflow(ctx workflow.Context, runtimeSeconds int) error {
	options := workflow.ActivityOptions{
		TaskQueue:              "TestTaskQueue",
		ScheduleToCloseTimeout: time.Second * 60,
		ScheduleToStartTimeout: time.Second * 60,
		StartToCloseTimeout:    time.Second * 60,
		HeartbeatTimeout:       time.Second * 5,
		WaitForCancellation:    false,
	}

	ctx = workflow.WithActivityOptions(ctx, options)
	err := workflow.ExecuteActivity(ctx, ExampleActivity, runtimeSeconds).Get(ctx, nil)
	if err != nil {
		log.Error("Unable to execute activity.")
		return err
	}

	return nil
}

func ExampleActivity(ctx context.Context, runtimeSeconds int) error {
	counter := 0
	for i := 0; i <= runtimeSeconds; i++ {

		select {
		case <-time.After(1 * time.Second):
			counter++
			activity.RecordHeartbeat(ctx, "")
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}
