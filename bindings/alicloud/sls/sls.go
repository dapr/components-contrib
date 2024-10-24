package sls

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/aliyun/aliyun-log-go-sdk/producer"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

type AliCloudSlsLogstorage struct {
	logger   logger.Logger
	producer *producer.Producer
	metadata SlsLogstorageMetadata
}

type SlsLogstorageMetadata struct {
	Endpoint        string `json:"endpoint" mapstructure:"endpoint"`
	AccessKeyID     string `json:"accessKeyID" mapstructure:"accessKeyID"`
	AccessKeySecret string `json:"accessKeySecret" mapstructure:"accessKeySecret"`
}

type Callback struct {
	s *AliCloudSlsLogstorage
}

// parse metadata field
func (s *AliCloudSlsLogstorage) Init(_ context.Context, metadata bindings.Metadata) error {
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

	s.producer.Start()
	return nil
}

func NewAliCloudSlsLogstorage(logger logger.Logger) bindings.OutputBinding {
	logger.Debug("initialized Sls log storage binding component")
	s := &AliCloudSlsLogstorage{
		logger: logger,
	}
	return s
}

func (s *AliCloudSlsLogstorage) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	// verify the metadata property
	if logProject := req.Metadata["project"]; logProject == "" {
		return nil, errors.New("SLS binding error: project property not supplied")
	}
	if logstore := req.Metadata["logstore"]; logstore == "" {
		return nil, errors.New("SLS binding error: logstore property not supplied")
	}
	if topic := req.Metadata["topic"]; topic == "" {
		return nil, errors.New("SLS binding error: topic property not supplied")
	}
	if source := req.Metadata["source"]; source == "" {
		return nil, errors.New("SLS binding error: source property not supplied")
	}

	log, err := s.parseLog(req)
	if err != nil {
		s.logger.Info(err)
		return nil, err
	}

	s.logger.Debug(log)
	callBack := &Callback{}
	err = s.producer.SendLogWithCallBack(req.Metadata["project"], req.Metadata["logstore"], req.Metadata["topic"], req.Metadata["source"], log, callBack)
	if err != nil {
		s.logger.Info(err)
		return nil, err
	}
	return nil, nil
}

// parse the log content
func (s *AliCloudSlsLogstorage) parseLog(req *bindings.InvokeRequest) (*sls.Log, error) {
	var logInfo map[string]string
	err := json.Unmarshal(req.Data, &logInfo)
	if err != nil {
		return nil, err
	}
	//nolint:gosec
	return producer.GenerateLog(uint32(time.Now().Unix()), logInfo), nil
}

func (s *AliCloudSlsLogstorage) parseMeta(meta bindings.Metadata) (*SlsLogstorageMetadata, error) {
	var m SlsLogstorageMetadata
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (s *AliCloudSlsLogstorage) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (callback *Callback) Success(result *producer.Result) {
}

func (callback *Callback) Fail(result *producer.Result) {
	msg := "unknown reason"
	if result.GetErrorMessage() != "" {
		msg = result.GetErrorMessage()
	}
	if result.GetErrorCode() != "" {
		callback.s.logger.Debug("Failed error code:", result.GetErrorCode())
	}

	callback.s.logger.Info("Log storage failed:", msg)
}

// GetComponentMetadata returns the metadata of the component.
func (s *AliCloudSlsLogstorage) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := SlsLogstorageMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}

func (s *AliCloudSlsLogstorage) Close() error {
	if s.producer != nil {
		return s.producer.Close(time.Second.Milliseconds() * 5)
	}

	return nil
}
