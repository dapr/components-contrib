package sls

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/aliyun/aliyun-log-go-sdk/producer"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

type AliCloudSlsLogstorage struct {
	logger   logger.Logger
	producer *producer.Producer
	metadata SlsLogstorageMetadata
}

type SlsLogstorageMetadata struct {
	Endpoint        string `json:"endpoint"`
	AccessKeyID     string `json:"accessKeyID"`
	AccessKeySecret string `json:"accessKeySecret"`
}

// parse metadata field
func (s *AliCloudSlsLogstorage) Init(metadata bindings.Metadata) error {
	m, err := s.parseMeta(metadata)
	if err != nil {
		return err
	}
	s.metadata = *m
	producerConfig := producer.GetDefaultProducerConfig()
	// the config properties in the component yaml file
	producerConfig.Endpoint = m.Endpoint
	producerConfig.AccessKeyID = m.AccessKeyID
	producerConfig.AccessKeySecret = m.AccessKeySecret
	s.producer = producer.InitProducer(producerConfig)
	return nil
}

func NewAliCloudSlsLogstorage(logger logger.Logger) *AliCloudSlsLogstorage {
	logger.Debug("new a Slslog component object")
	s := &AliCloudSlsLogstorage{
		logger: logger,
	}
	return s
}

func (s *AliCloudSlsLogstorage) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	// verify the metadata property
	if logProject := req.Metadata["project"]; logProject == "" {
		return nil, fmt.Errorf("SLS binding error: project property not supplied")
	}
	if logstore := req.Metadata["logstore"]; logstore == "" {
		return nil, fmt.Errorf("SLS binding error: logstore property not supplied")
	}
	if topic := req.Metadata["topic"]; topic == "" {
		return nil, fmt.Errorf("SLS binding error: topic property not supplied")
	}
	if source := req.Metadata["source"]; source == "" {
		return nil, fmt.Errorf("SLS binding error: source property not supplied")
	}

	ch := make(chan os.Signal)
	signal.Notify(ch, os.Kill, os.Interrupt)

	// Start producer instancce
	s.producer.Start()
	log, err := s.parseLog(req)
	if err != nil {
		s.logger.Info(err)
		return nil, err
	}

	s.logger.Debug(log)
	err = s.producer.SendLog(req.Metadata["project"], req.Metadata["logstore"], req.Metadata["topic"], req.Metadata["source"], log)
	if err != nil {
		s.logger.Info(err)
		return nil, err
	}
	s.producer.SafeClose()
	return nil, err
}

// parse the log content
func (s *AliCloudSlsLogstorage) parseLog(req *bindings.InvokeRequest) (*sls.Log, error) {
	var logInfo map[string]string
	err := json.Unmarshal(req.Data, &logInfo)
	if err != nil {
		return nil, err
	}
	var logTime uint32
	// if no timestamp , use current time
	if logInfo["timestamp"] != "" {
		t, _ := strconv.Atoi(logInfo["timestamp"])
		logTime = uint32(t)
	} else {
		logTime = uint32(time.Now().Unix())
	}

	return producer.GenerateLog(logTime, logInfo), nil
}

func (s *AliCloudSlsLogstorage) parseMeta(metadata bindings.Metadata) (*SlsLogstorageMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var m SlsLogstorageMetadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (s *AliCloudSlsLogstorage) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}
