package kafka

import (
	"encoding/binary"
	"encoding/json"
	"testing"
	"time"

	"github.com/IBM/sarama"
	gomock "github.com/golang/mock/gomock"
	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
	"github.com/stretchr/testify/require"

	mock_srclient "github.com/dapr/components-contrib/common/component/kafka/mocks"
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
	testSchema1 = `{"type": "record", "name": "cupcake", "fields": [{"name": "flavor", "type": "string"}, {"name": "created_date", "type": ["null",{"type": "long","logicalType": "timestamp-millis"}],"default": null}]}`
	testValue1  = map[string]interface{}{"flavor": "chocolate", "created_date": float64(time.Now().UnixMilli())}
	invValue    = map[string]string{"xxx": "chocolate"}
)

func TestDeserializeValue(t *testing.T) {
	registry := srclient.CreateMockSchemaRegistryClient("http://localhost:8081")
	schema, _ := registry.CreateSchema("my-topic-value", testSchema1, srclient.Avro)
	handlerConfig := SubscriptionHandlerConfig{
		IsBulkSubscribe: false,
		ValueSchemaType: Avro,
	}
	k := Kafka{
		srClient:             registry,
		schemaCachingEnabled: true,
	}

	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID())) //nolint:gosec

	valJSON, _ := json.Marshal(testValue1)
	codec, _ := goavro.NewCodecForStandardJSONFull(testSchema1)
	native, _, _ := codec.NativeFromTextual(valJSON)
	valueBytes, _ := codec.BinaryFromNative(nil, native)

	var recordValue []byte
	recordValue = append(recordValue, byte(0))
	recordValue = append(recordValue, schemaIDBytes...)
	recordValue = append(recordValue, valueBytes...)

	t.Run("Schema found, return value", func(t *testing.T) {
		msg := sarama.ConsumerMessage{
			Key:   []byte("my_key"),
			Value: recordValue,
			Topic: "my-topic",
		}
		act, err := k.DeserializeValue(&msg, handlerConfig)
		var actMap map[string]any
		json.Unmarshal(act, &actMap)
		require.Equal(t, testValue1, actMap)
		require.NoError(t, err)
	})

	t.Run("Data null, return as JSON null", func(t *testing.T) {
		msg := sarama.ConsumerMessage{
			Key:   []byte("my_key"),
			Value: nil,
			Topic: "my-topic",
		}
		act, err := k.DeserializeValue(&msg, handlerConfig)
		require.Equal(t, []byte("null"), act)
		require.NoError(t, err)
	})

	t.Run("Invalid too short data, return error", func(t *testing.T) {
		msg := sarama.ConsumerMessage{
			Key:   []byte("my_key"),
			Value: []byte("xxxx"),
			Topic: "my-topic",
		}
		_, err := k.DeserializeValue(&msg, handlerConfig)

		require.Error(t, err)
	})

	t.Run("Invalid Schema ID, return error", func(t *testing.T) {
		msg := sarama.ConsumerMessage{
			Key:   []byte("my_key"),
			Value: []byte("xxxxx"),
			Topic: "my-topic",
		}
		_, err := k.DeserializeValue(&msg, handlerConfig)

		require.Error(t, err)
	})

	t.Run("Invalid data, return error", func(t *testing.T) {
		var invalidVal []byte
		invalidVal = append(invalidVal, byte(0))
		invalidVal = append(invalidVal, schemaIDBytes...)
		invalidVal = append(invalidVal, []byte("xxx")...)

		msg := sarama.ConsumerMessage{
			Key:   []byte("my_key"),
			Value: invalidVal,
			Topic: "my-topic",
		}
		_, err := k.DeserializeValue(&msg, handlerConfig)

		require.Error(t, err)
	})

	t.Run("Missing Schema Registry settings, return error", func(t *testing.T) {
		kInv := Kafka{
			srClient:             nil,
			schemaCachingEnabled: true,
		}
		msg := sarama.ConsumerMessage{
			Key:   []byte("my_key"),
			Value: recordValue,
			Topic: "my-topic",
		}
		_, err := kInv.DeserializeValue(&msg, handlerConfig)
		require.Error(t, err, "schema registry details not set")
	})
}

func assertValueSerialized(t *testing.T, act []byte, valJSON []byte, schema *srclient.Schema) {
	require.NotEqual(t, act, valJSON)

	actSchemaID := int(binary.BigEndian.Uint32(act[1:5]))
	codec, _ := goavro.NewCodecForStandardJSONFull(schema.Schema())
	native, _, _ := codec.NativeFromBinary(act[5:])
	actJSON, _ := codec.TextualFromNative(nil, native)
	var actMap map[string]any
	json.Unmarshal(actJSON, &actMap)

	require.Equal(t, schema.ID(), actSchemaID)
	require.Equal(t, testValue1, actMap)
}

func TestSerializeValueCachingDisabled(t *testing.T) {
	registry := srclient.CreateMockSchemaRegistryClient("http://localhost:8081")
	schema, _ := registry.CreateSchema("my-topic-value", testSchema1, srclient.Avro)

	k := Kafka{
		srClient:             registry,
		schemaCachingEnabled: false,
	}

	t.Run("valueSchemaType not set, leave value as is", func(t *testing.T) {
		valJSON, _ := json.Marshal(testValue1)

		act, err := k.SerializeValue("my-topic", valJSON, map[string]string{})

		require.Equal(t, valJSON, act)
		require.NoError(t, err)
	})

	t.Run("valueSchemaType set to None, leave value as is", func(t *testing.T) {
		valJSON, _ := json.Marshal(testValue1)

		act, err := k.SerializeValue("my-topic", valJSON, map[string]string{"valueSchemaType": "None"})

		require.Equal(t, valJSON, act)
		require.NoError(t, err)
	})

	t.Run("valueSchemaType set to None, leave value as is", func(t *testing.T) {
		valJSON, _ := json.Marshal(testValue1)

		act, err := k.SerializeValue("my-topic", valJSON, map[string]string{"valueSchemaType": "NONE"})

		require.Equal(t, valJSON, act)
		require.NoError(t, err)
	})

	t.Run("valueSchemaType invalid, return error", func(t *testing.T) {
		valJSON, _ := json.Marshal(testValue1)

		_, err := k.SerializeValue("my-topic", valJSON, map[string]string{"valueSchemaType": "xx"})

		require.Error(t, err, "error parsing schema type. 'xx' is not a supported value")
	})

	t.Run("schema found, serialize value as Avro binary", func(t *testing.T) {
		valJSON, _ := json.Marshal(testValue1)
		act, err := k.SerializeValue("my-topic", valJSON, map[string]string{"valueSchemaType": "Avro"})
		assertValueSerialized(t, act, valJSON, schema)
		require.NoError(t, err)
	})

	t.Run("value published 'null', no error", func(t *testing.T) {
		act, err := k.SerializeValue("my-topic", []byte("null"), map[string]string{"valueSchemaType": "Avro"})

		require.Nil(t, act)
		require.NoError(t, err)
	})

	t.Run("value published nil, no error", func(t *testing.T) {
		act, err := k.SerializeValue("my-topic", nil, map[string]string{"valueSchemaType": "Avro"})

		require.Nil(t, act)
		require.NoError(t, err)
	})

	t.Run("invalid data, return error", func(t *testing.T) {
		valJSON, _ := json.Marshal(invValue)
		_, err := k.SerializeValue("my-topic", valJSON, map[string]string{"valueSchemaType": "Avro"})

		require.Error(t, err, "cannot decode textual record \"cupcake\": cannot decode textual map: cannot determine codec: \"xxx\"")
	})
}

func TestSerializeValueCachingEnabled(t *testing.T) {
	registry := srclient.CreateMockSchemaRegistryClient("http://localhost:8081")
	schema, _ := registry.CreateSchema("my-topic-value", testSchema1, srclient.Avro)

	k := Kafka{
		srClient:             registry,
		schemaCachingEnabled: true,
		latestSchemaCache:    make(map[string]SchemaCacheEntry),
		latestSchemaCacheTTL: time.Minute * 5,
	}

	t.Run("valueSchemaType not set, leave value as is", func(t *testing.T) {
		valJSON, _ := json.Marshal(testValue1)
		act, err := k.SerializeValue("my-topic", valJSON, map[string]string{})
		require.Equal(t, valJSON, act)
		require.NoError(t, err)
	})

	t.Run("schema found, serialize value as Avro binary", func(t *testing.T) {
		valJSON, _ := json.Marshal(testValue1)
		act, err := k.SerializeValue("my-topic", valJSON, map[string]string{"valueSchemaType": "Avro"})
		assertValueSerialized(t, act, valJSON, schema)
		require.NoError(t, err)
	})
}

func TestLatestSchemaCaching(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	registry := srclient.CreateMockSchemaRegistryClient("http://locahost:8081")
	m := mock_srclient.NewMockISchemaRegistryClient(ctrl)
	schema, _ := registry.CreateSchema("my-topic-value", testSchema1, srclient.Avro)

	t.Run("Caching enabled, call GetLatestSchema() only once", func(t *testing.T) {
		k := Kafka{
			srClient:             m,
			schemaCachingEnabled: true,
			latestSchemaCache:    make(map[string]SchemaCacheEntry),
			latestSchemaCacheTTL: time.Second * 10,
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
