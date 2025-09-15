package kafka

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/golang/mock/gomock"
	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	awsAuth "github.com/dapr/components-contrib/common/aws/auth"
	mock_srclient "github.com/dapr/components-contrib/common/component/kafka/mocks"
	"github.com/dapr/kit/logger"
)

func TestGetValueSchemaType(t *testing.T) {
	t.Run("No Metadata, return None", func(t *testing.T) {
		act, err := GetValueSchemaType(nil)
		require.Equal(t, None, act)
		require.NoError(t, err)
	})

	t.Run("No valueSchemaType, return None", func(t *testing.T) {
		act, err := GetValueSchemaType(make(map[string]string))
		require.Equal(t, None, act)
		require.NoError(t, err)
	})

	t.Run("valueSchemaType='AVRO', return AVRO", func(t *testing.T) {
		act, err := GetValueSchemaType(map[string]string{"valueSchemaType": "AVRO"})
		require.Equal(t, Avro, act)
		require.NoError(t, err)
	})

	t.Run("valueSchemaType='None', return None", func(t *testing.T) {
		act, err := GetValueSchemaType(map[string]string{"valueSchemaType": "None"})
		require.Equal(t, None, act)
		require.NoError(t, err)
	})

	t.Run("valueSchemaType='XXX', return Error", func(t *testing.T) {
		_, err := GetValueSchemaType(map[string]string{"valueSchemaType": "XXX"})
		require.Error(t, err)
	})
}

var (
	now         = time.Now()
	testSchema1 = `{
	"type": "record", 
	"name": "cupcake", 
	"fields": [
		{"name": "flavor", "type": "string"}, 
		{"name": "weight", "type": {"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 2}}, 
		{"name": "created_date", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": null}
		]
	}`
	testValue1         = map[string]any{"flavor": "chocolate", "weight": "100.24", "created_date": float64(now.UnixMilli())}
	testValue1AvroJSON = map[string]any{"flavor": "chocolate", "weight": big.NewRat(10024, 100), "created_date": goavro.Union("long.timestamp-millis", float64(now.UnixMilli()))}
	invValue           = map[string]string{"xxx": "chocolate"}
)

func TestDeserializeValue(t *testing.T) {
	handlerConfig := SubscriptionHandlerConfig{
		IsBulkSubscribe: false,
		ValueSchemaType: Avro,
	}

	registryJSON := srclient.CreateMockSchemaRegistryClient("http://localhost:8081")
	kJSON := Kafka{
		srClient:             registryJSON,
		schemaCachingEnabled: true,
		logger:               logger.NewLogger("kafka_test"),
	}
	kJSON.srClient.CodecJsonEnabled(true)
	schemaJSON, _ := registryJSON.CreateSchema("my-topic-value", testSchema1, srclient.Avro)

	// set up for Standard JSON
	codec, _ := goavro.NewCodecForStandardJSONFull(testSchema1)
	valJSON, _ := json.Marshal(testValue1)
	native, _, _ := codec.NativeFromTextual(valJSON)
	valueBytesFromJSON, _ := codec.BinaryFromNative(nil, native)
	recordValueFromJSON := formatByteRecord(schemaJSON.ID(), valueBytesFromJSON)

	// setup for Avro JSON
	registryAvroJSON := srclient.CreateMockSchemaRegistryClient("http://localhost:8081")
	kAvroJSON := Kafka{
		srClient:             registryAvroJSON,
		schemaCachingEnabled: true,
		logger:               logger.NewLogger("kafka_test"),
	}
	kAvroJSON.srClient.CodecJsonEnabled(false)
	schemaAvroJSON, _ := registryAvroJSON.CreateSchema("my-topic-value", testSchema1, srclient.Avro)

	codecAvroJSON, _ := goavro.NewCodec(testSchema1)
	valueBytesFromAvroJSON, _ := codecAvroJSON.BinaryFromNative(nil, testValue1AvroJSON)
	valueAvroJSON, _ := codecAvroJSON.TextualFromNative(nil, testValue1AvroJSON)
	recordValueFromAvroJSON := formatByteRecord(schemaAvroJSON.ID(), valueBytesFromAvroJSON)

	t.Run("Schema found, return value", func(t *testing.T) {
		msg := sarama.ConsumerMessage{
			Key:   []byte("my_key"),
			Value: recordValueFromJSON,
			Topic: "my-topic",
		}
		act, err := kJSON.DeserializeValue(&msg, handlerConfig)
		var actMap map[string]any
		json.Unmarshal(act, &actMap)
		require.Equal(t, testValue1, actMap)
		require.NoError(t, err)
	})

	t.Run("Schema found and useAvroJson, return value as Avro Json", func(t *testing.T) {
		msg := sarama.ConsumerMessage{
			Key:   []byte("my_key"),
			Value: recordValueFromAvroJSON,
			Topic: "my-topic",
		}

		actText, err := kAvroJSON.DeserializeValue(&msg, handlerConfig)
		// test if flaky if comparing the textual version. Convert to native for comparison
		exp, _, _ := codecAvroJSON.NativeFromTextual(valueAvroJSON)
		act, _, _ := codecAvroJSON.NativeFromTextual(actText)
		require.Equal(t, exp, act)
		require.NoError(t, err)
	})

	t.Run("Data null, return as JSON null", func(t *testing.T) {
		msg := sarama.ConsumerMessage{
			Key:   []byte("my_key"),
			Value: nil,
			Topic: "my-topic",
		}
		act, err := kJSON.DeserializeValue(&msg, handlerConfig)
		require.Equal(t, []byte("null"), act)
		require.NoError(t, err)
	})

	t.Run("Invalid too short data, return error", func(t *testing.T) {
		msg := sarama.ConsumerMessage{
			Key:   []byte("my_key"),
			Value: []byte("xxxx"),
			Topic: "my-topic",
		}
		_, err := kJSON.DeserializeValue(&msg, handlerConfig)

		require.Error(t, err)
	})

	t.Run("Invalid Schema ID, return error", func(t *testing.T) {
		msg := sarama.ConsumerMessage{
			Key:   []byte("my_key"),
			Value: []byte("xxxxx"),
			Topic: "my-topic",
		}
		_, err := kJSON.DeserializeValue(&msg, handlerConfig)

		require.Error(t, err)
	})

	t.Run("Invalid data, return error", func(t *testing.T) {
		var invalidVal []byte
		invalidVal = append(invalidVal, byte(0))
		invalidVal = append(invalidVal, recordValueFromJSON[0:5]...)
		invalidVal = append(invalidVal, []byte("xxx")...)

		msg := sarama.ConsumerMessage{
			Key:   []byte("my_key"),
			Value: invalidVal,
			Topic: "my-topic",
		}
		_, err := kJSON.DeserializeValue(&msg, handlerConfig)

		require.Error(t, err)
	})

	t.Run("Missing Schema Registry settings, return error", func(t *testing.T) {
		kInv := Kafka{
			srClient:             nil,
			schemaCachingEnabled: true,
		}
		msg := sarama.ConsumerMessage{
			Key:   []byte("my_key"),
			Value: recordValueFromJSON,
			Topic: "my-topic",
		}
		_, err := kInv.DeserializeValue(&msg, handlerConfig)
		require.Error(t, err, "schema registry details not set")
	})
}

func formatByteRecord(schemaID int, valueBytes []byte) []byte {
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schemaID)) //nolint:gosec

	var recordValue []byte
	recordValue = append(recordValue, byte(0))
	recordValue = append(recordValue, schemaIDBytes...)
	recordValue = append(recordValue, valueBytes...)
	return recordValue
}

func assertValueSerialized(t *testing.T, act []byte, expJSON []byte, schema *srclient.Schema) {
	require.NotEqual(t, expJSON, act)

	actSchemaID := int(binary.BigEndian.Uint32(act[1:5]))
	codec, _ := goavro.NewCodecForStandardJSONFull(schema.Schema())
	native, _, _ := codec.NativeFromBinary(act[5:])
	actJSON, _ := codec.TextualFromNative(nil, native)
	var actMap map[string]any
	json.Unmarshal(actJSON, &actMap)
	var expMap map[string]any
	json.Unmarshal(expJSON, &expMap)

	require.Equal(t, schema.ID(), actSchemaID)
	require.Equal(t, expMap, actMap)
}

func TestSerializeValueCachingDisabled(t *testing.T) {
	registryJSON := srclient.CreateMockSchemaRegistryClient("http://localhost:8081")
	registryJSON.CodecJsonEnabled(true)
	schemaJSON, _ := registryJSON.CreateSchema("my-topic-value", testSchema1, srclient.Avro)

	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schemaJSON.ID())) //nolint:gosec

	// set up for Avro JSON
	registryAvroJSON := srclient.CreateMockSchemaRegistryClient("http://localhost:8081")
	registryAvroJSON.CodecJsonEnabled(false)
	codecAvroJSON, _ := goavro.NewCodec(testSchema1)
	schemaAvroJSON, _ := registryAvroJSON.CreateSchema("my-topic-value", testSchema1, srclient.Avro)
	schemaAvroJSONIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaAvroJSONIDBytes, uint32(schemaAvroJSON.ID())) //nolint:gosec
	valueBytesFromAvroJSON, _ := codecAvroJSON.BinaryFromNative(nil, testValue1AvroJSON)
	valueAvroJSON, _ := codecAvroJSON.TextualFromNative(nil, testValue1AvroJSON)
	var recordValueFromAvroJSON []byte
	recordValueFromAvroJSON = append(recordValueFromAvroJSON, byte(0))
	recordValueFromAvroJSON = append(recordValueFromAvroJSON, schemaAvroJSONIDBytes...)
	recordValueFromAvroJSON = append(recordValueFromAvroJSON, valueBytesFromAvroJSON...)

	kJSON := Kafka{
		srClient:             registryJSON,
		schemaCachingEnabled: false,
		logger:               logger.NewLogger("kafka_test"),
	}

	kAvroJSON := Kafka{
		srClient:             registryAvroJSON,
		schemaCachingEnabled: false,
		logger:               logger.NewLogger("kafka_test"),
	}

	t.Run("valueSchemaType not set, leave value as is", func(t *testing.T) {
		valJSON, _ := json.Marshal(testValue1)
		act, err := kJSON.SerializeValue("my-topic", valJSON, map[string]string{})
		require.JSONEq(t, string(valJSON), string(act))
		require.NoError(t, err)
	})

	t.Run("valueSchemaType set to None, leave value as is", func(t *testing.T) {
		valJSON, _ := json.Marshal(testValue1)
		act, err := kJSON.SerializeValue("my-topic", valJSON, map[string]string{"valueSchemaType": "None"})
		require.JSONEq(t, string(valJSON), string(act))
		require.NoError(t, err)
	})

	t.Run("valueSchemaType set to None, leave value as is", func(t *testing.T) {
		valJSON, _ := json.Marshal(testValue1)
		act, err := kJSON.SerializeValue("my-topic", valJSON, map[string]string{"valueSchemaType": "NONE"})
		require.JSONEq(t, string(valJSON), string(act))
		require.NoError(t, err)
	})

	t.Run("valueSchemaType invalid, return error", func(t *testing.T) {
		valJSON, _ := json.Marshal(testValue1)
		_, err := kJSON.SerializeValue("my-topic", valJSON, map[string]string{"valueSchemaType": "xx"})
		require.Error(t, err, "error parsing schema type. 'xx' is not a supported value")
	})

	t.Run("schema found, serialize value as Avro binary", func(t *testing.T) {
		valJSON, _ := json.Marshal(testValue1)
		act, err := kJSON.SerializeValue("my-topic", valJSON, map[string]string{"valueSchemaType": "Avro"})
		assertValueSerialized(t, act, valJSON, schemaJSON)
		require.NoError(t, err)
	})

	t.Run("schema found, useAvroJson=true, serialize value as Avro binary", func(t *testing.T) {
		act, err := kAvroJSON.SerializeValue("my-topic", valueAvroJSON, map[string]string{"valueSchemaType": "Avro"})
		require.Equal(t, act[:5], recordValueFromAvroJSON[:5])
		require.NoError(t, err)
	})

	t.Run("value published 'null', no error", func(t *testing.T) {
		act, err := kJSON.SerializeValue("my-topic", []byte("null"), map[string]string{"valueSchemaType": "Avro"})
		require.Nil(t, act)
		require.NoError(t, err)
	})

	t.Run("value published nil, no error", func(t *testing.T) {
		act, err := kJSON.SerializeValue("my-topic", nil, map[string]string{"valueSchemaType": "Avro"})
		require.Nil(t, act)
		require.NoError(t, err)
	})

	t.Run("invalid data, return error", func(t *testing.T) {
		valJSON, _ := json.Marshal(invValue)
		_, err := kJSON.SerializeValue("my-topic", valJSON, map[string]string{"valueSchemaType": "Avro"})
		require.Error(t, err, "cannot decode textual record \"cupcake\": cannot decode textual map: cannot determine codec: \"xxx\"")
	})
}

func TestSerializeValueCachingEnabled(t *testing.T) {
	registry := srclient.CreateMockSchemaRegistryClient("http://localhost:8081")
	registry.CodecJsonEnabled(true)
	schema, _ := registry.CreateSchema("my-topic-value", testSchema1, srclient.Avro)

	k := Kafka{
		srClient:             registry,
		schemaCachingEnabled: true,
		latestSchemaCache:    make(map[string]SchemaCacheEntry),
		latestSchemaCacheTTL: time.Minute * 5,
		logger:               logger.NewLogger("kafka_test"),
	}

	t.Run("valueSchemaType not set, leave value as is", func(t *testing.T) {
		valJSON, _ := json.Marshal(testValue1)
		act, err := k.SerializeValue("my-topic", valJSON, map[string]string{})
		require.JSONEq(t, string(valJSON), string(act))
		require.NoError(t, err)
	})

	t.Run("schema found, serialize value as Avro binary", func(t *testing.T) {
		valJSON, _ := json.Marshal(testValue1)
		act, err := k.SerializeValue("my-topic", valJSON, map[string]string{"valueSchemaType": "Avro"})
		assertValueSerialized(t, act, valJSON, schema)
		require.NoError(t, err)
	})

	t.Run("serialize with complex avro schema", func(t *testing.T) {
		testSchemaOcr := `{
  "type": "record",
  "name": "ocr_requested",
  "namespace": "foo.cmd.image_processing",
  "fields": [
    {
      "name": "id",
      "type": "string",
      "doc": "Idempotency key"
    },
    {
      "name": "document_metadata",
      "type": {
        "type": "record",
        "name": "DocumentMetadata",
        "fields": [
          {
            "name": "content_type",
            "type": "string"
          },
          {
            "name": "original_filename",
            "type": ["null", "string"],
            "default": null
          },
          {
            "name": "source",
            "type": {
              "type": "enum",
              "name": "DocumentSource",
              "symbols": [
                "Unknown",
                "Import",
                "PatientUpload",
                "UserUpload",
                "UserUploadFax"
              ]
            }
          },
          {
            "name": "type",
            "type": {
              "type": "enum",
              "name": "DocumentType",
              "symbols": ["Unknown", "InsuranceCard", "MiscReport"]
            }
          }
        ]
      }
    },
    {
      "name": "s3_path",
      "type": {
        "type": "record",
        "name": "S3Path",
        "fields": [
          { "name": "bucket", "type": "string" },
          { "name": "key", "type": "string" }
        ]
      }
    }
  ]
}`
		schemaOcr, _ := registry.CreateSchema("my-ocr-topic-value", testSchemaOcr, srclient.Avro)
		valueOcr := map[string]any{"id": "123", "document_metadata": map[string]any{"content_type": "application/pdf", "original_filename": nil, "source": "UserUpload", "type": "InsuranceCard"}, "s3_path": map[string]any{"bucket": "test-bucket", "key": "test-key"}}
		valJSONOcr, _ := json.Marshal(valueOcr)
		act, err := k.SerializeValue("my-ocr-topic", valJSONOcr, map[string]string{"valueSchemaType": "Avro"})
		assertValueSerialized(t, act, valJSONOcr, schemaOcr)
		require.NoError(t, err)
	})
}

func TestLatestSchemaCaching(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	registry := srclient.CreateMockSchemaRegistryClient("http://locahost:8081")
	registry.CodecJsonEnabled(true)
	m := mock_srclient.NewMockISchemaRegistryClient(ctrl)
	schema, _ := registry.CreateSchema("my-topic-value", testSchema1, srclient.Avro)

	t.Run("Caching enabled, call GetLatestSchema() only once", func(t *testing.T) {
		k := Kafka{
			srClient:             m,
			schemaCachingEnabled: true,
			latestSchemaCache:    make(map[string]SchemaCacheEntry),
			latestSchemaCacheTTL: time.Second * 10,
			logger:               logger.NewLogger("kafka_test"),
		}

		m.EXPECT().GetLatestSchema(gomock.Eq("my-topic-value")).Return(schema, nil).Times(1)

		valJSON, _ := json.Marshal(testValue1)

		act, err := k.SerializeValue("my-topic", valJSON, map[string]string{"valueSchemaType": "Avro"})
		assertValueSerialized(t, act, valJSON, schema)
		require.NoError(t, err)

		// Call a 2nd time within TTL and make sure it's not called again
		act, err = k.SerializeValue("my-topic", valJSON, map[string]string{"valueSchemaType": "Avro"})
		assertValueSerialized(t, act, valJSON, schema)
		require.NoError(t, err)
	})

	t.Run("Caching enabled, when cache entry expires, call GetLatestSchema() again", func(t *testing.T) {
		k := Kafka{
			srClient:             m,
			schemaCachingEnabled: true,
			latestSchemaCache:    make(map[string]SchemaCacheEntry),
			latestSchemaCacheTTL: time.Second * 1,
			logger:               logger.NewLogger("kafka_test"),
		}

		m.EXPECT().GetLatestSchema(gomock.Eq("my-topic-value")).Return(schema, nil).Times(2)

		valJSON, _ := json.Marshal(testValue1)

		act, err := k.SerializeValue("my-topic", valJSON, map[string]string{"valueSchemaType": "Avro"})
		assertValueSerialized(t, act, valJSON, schema)
		require.NoError(t, err)

		time.Sleep(2 * time.Second)

		// Call a 2nd time within TTL and make sure it's not called again
		act, err = k.SerializeValue("my-topic", valJSON, map[string]string{"valueSchemaType": "Avro"})
		assertValueSerialized(t, act, valJSON, schema)
		require.NoError(t, err)
	})

	t.Run("Caching disabled, call GetLatestSchema() twice", func(t *testing.T) {
		k := Kafka{
			srClient:             m,
			schemaCachingEnabled: false,
			latestSchemaCache:    make(map[string]SchemaCacheEntry),
			latestSchemaCacheTTL: 0,
			logger:               logger.NewLogger("kafka_test"),
		}

		m.EXPECT().GetLatestSchema(gomock.Eq("my-topic-value")).Return(schema, nil).Times(2)
		valJSON, _ := json.Marshal(testValue1)
		act, err := k.SerializeValue("my-topic", valJSON, map[string]string{"valueSchemaType": "Avro"})

		assertValueSerialized(t, act, valJSON, schema)
		require.NoError(t, err)

		// Call a 2nd time within TTL and make sure it's not called again
		act, err = k.SerializeValue("my-topic", valJSON, map[string]string{"valueSchemaType": "Avro"})

		assertValueSerialized(t, act, valJSON, schema)
		require.NoError(t, err)
	})
}

func TestValidateAWS(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
		expected awsAuth.Options
		err      error
	}{
		{
			name: "Valid metadata with all fields without aws prefix",
			metadata: map[string]string{
				"region":        "us-east-1",
				"accessKey":     "testAccessKey",
				"secretKey":     "testSecretKey",
				"assumeRoleArn": "testRoleArn",
				"sessionName":   "testSessionName",
				"sessionToken":  "testSessionToken",
			},
			expected: awsAuth.Options{
				Region:                "us-east-1",
				AccessKey:             "testAccessKey",
				SecretKey:             "testSecretKey",
				AssumeRoleArn:         "testRoleArn",
				AssumeRoleSessionName: "testSessionName",
				SessionToken:          "testSessionToken",
			},
			err: nil,
		},
		{
			name: "Fallback to aws-prefixed fields with aws prefix",
			metadata: map[string]string{
				"awsRegion":         "us-west-2",
				"awsAccessKey":      "awsAccessKey",
				"awsSecretKey":      "awsSecretKey",
				"awsIamRoleArn":     "awsRoleArn",
				"awsStsSessionName": "awsSessionName",
				"awsSessionToken":   "awsSessionToken",
			},
			expected: awsAuth.Options{
				Region:                "us-west-2",
				AccessKey:             "awsAccessKey",
				SecretKey:             "awsSecretKey",
				AssumeRoleArn:         "awsRoleArn",
				AssumeRoleSessionName: "awsSessionName",
				SessionToken:          "awsSessionToken",
			},
			err: nil,
		},
		{
			name: "Missing region field",
			metadata: map[string]string{
				"accessKey": "key",
				"secretKey": "secret",
			},
			expected: awsAuth.Options{},
			err:      errors.New("metadata property AWSRegion is missing"),
		},
		{
			name:     "Empty metadata",
			metadata: map[string]string{},
			expected: awsAuth.Options{},
			err:      errors.New("metadata property AWSRegion is missing"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &Kafka{}
			result, err := k.ValidateAWS(tt.metadata)
			if tt.err != nil {
				require.EqualError(t, err, tt.err.Error())
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInitConsumerGroupRebalanceStrategy(t *testing.T) {
	tests := []struct {
		name             string
		metadata         map[string]string
		expectedStrategy string
	}{
		{
			name:             "missing consumerGroupRebalanceStrategy property defaults to Range",
			metadata:         map[string]string{},
			expectedStrategy: "range",
		},
		{
			name: "empty consumerGroupRebalanceStrategy property defaults to Range",
			metadata: map[string]string{
				"consumerGroupRebalanceStrategy": "",
			},
			expectedStrategy: "range",
		},
		{
			name: "valid sticky strategy",
			metadata: map[string]string{
				"consumerGroupRebalanceStrategy": "sticky",
			},
			expectedStrategy: "sticky",
		},
		{
			name: "valid roundrobin strategy",
			metadata: map[string]string{
				"consumerGroupRebalanceStrategy": "roundrobin",
			},
			expectedStrategy: "roundrobin",
		},
		{
			name: "valid range strategy",
			metadata: map[string]string{
				"consumerGroupRebalanceStrategy": "range",
			},
			expectedStrategy: "range",
		},
		{
			name: "case insensitive strategy",
			metadata: map[string]string{
				"consumerGroupRebalanceStrategy": "sTickY",
			},
			expectedStrategy: "sticky",
		},
		{
			name: "case insensitive strategy default",
			metadata: map[string]string{
				"consumerGroupRebalanceStrategy": "Range",
			},
			expectedStrategy: "range",
		},
		{
			name: "invalid strategy defaults to Range with warning",
			metadata: map[string]string{
				"consumerGroupRebalanceStrategy": "invalid",
			},
			expectedStrategy: "range",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create Kafka instance with logger
			k := &Kafka{
				logger: logger.NewLogger("kafka_test"),
			}

			// Create sarama config
			config := sarama.NewConfig()

			// Call the method under test
			k.initConsumerGroupRebalanceStrategy(config, tt.metadata)

			// Verify the strategy was set correctly
			require.Len(t, config.Consumer.Group.Rebalance.GroupStrategies, 1, "Expected exactly one rebalance strategy")

			assert.Equal(t, tt.expectedStrategy, config.Consumer.Group.Rebalance.GroupStrategies[0].Name())

			// Note: Warning verification would require a more sophisticated mock
			// For now, we just verify the strategy is set correctly
		})
	}
}
