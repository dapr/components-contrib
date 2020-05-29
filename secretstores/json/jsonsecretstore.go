// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package jsonsecretstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/dapr/pkg/logger"
)

const (
	/* #nosec */
	enableSecretStoreVariable = "DAPR_ENABLE_JSON_SECRET_STORE"
)

type jsonSecretStoreMetaData struct {
	SecretsFile     string `json:"secretsFile"`
	NestedSeparator string `json:"nestedSeparator"`
}

type jsonSecretStore struct {
	secretsFile     string
	nestedSeparator string
	currenContext   []string
	currentPath     string
	secrets         map[string]string
	readJSONFileFn  func(secretsFile string) (map[string]interface{}, error)
	logger          logger.Logger
}

// NewJSONSecretStore returns a new JSON secret store
func NewJSONSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &jsonSecretStore{
		logger: logger,
	}
}

// Init creates a JSON secret store
func (j *jsonSecretStore) Init(metadata secretstores.Metadata) error {
	enable := os.Getenv(enableSecretStoreVariable)
	if enable != "1" {
		return fmt.Errorf("jsonsecretstore must be explicitly enabled setting %s environment variable value to 1", enableSecretStoreVariable)
	}

	meta, err := j.getJSONSecretStoreMetadata(metadata)
	if err != nil {
		return err
	}

	if len(meta.NestedSeparator) == 0 {
		j.nestedSeparator = ":"
	}

	if j.readJSONFileFn == nil {
		j.readJSONFileFn = j.readJSONFile
	}

	j.secrets = map[string]string{}

	jsonConfig, err := j.readJSONFileFn(meta.SecretsFile)
	if err != nil {
		return err
	}

	j.visitJSONObject(jsonConfig)

	return nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values
func (j *jsonSecretStore) GetSecret(req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
	secretValue, exists := j.secrets[req.Name]
	if !exists {
		return secretstores.GetSecretResponse{}, fmt.Errorf("secret %s not found", req.Name)
	}

	return secretstores.GetSecretResponse{
		Data: map[string]string{
			req.Name: secretValue,
		},
	}, nil
}

func (j *jsonSecretStore) visitJSONObject(jsonConfig map[string]interface{}) error {
	for key, element := range jsonConfig {
		j.enterContext(key)
		err := j.visitProperty(element)
		if err != nil {
			return err
		}
		j.exitContext()
	}
	return nil
}

func (j *jsonSecretStore) enterContext(context string) {
	j.currenContext = append(j.currenContext, context)
	j.currentPath = j.combine(j.currenContext)
}

func (j *jsonSecretStore) visitPrimitive(context string) error {
	key := j.currentPath
	_, exists := j.secrets[key]

	if exists {
		return errors.New("duplicate key")
	}

	j.secrets[key] = context

	return nil
}

func (j *jsonSecretStore) visitArray(array []interface{}) error {
	for i := 0; i < len(array); i++ {
		j.enterContext(strconv.Itoa(i))
		err := j.visitProperty(array[i])
		if err != nil {
			return err
		}
		j.exitContext()
	}
	return nil
}

func (j *jsonSecretStore) visitProperty(property interface{}) error {
	switch v := property.(type) {
	case map[string]interface{}:
		return j.visitJSONObject(v)
	case []interface{}:
		return j.visitArray(v)
	case bool, string, int, float32, float64, byte, nil:
		return j.visitPrimitive(fmt.Sprintf("%s", v))
	default:
		return errors.New("couldn't parse property")
	}
}

func (j *jsonSecretStore) exitContext() {
	j.pop()
	j.currentPath = j.combine(j.currenContext)
}

func (j *jsonSecretStore) pop() {
	n := len(j.currenContext) - 1 // Top element
	j.currenContext[n] = ""
	j.currenContext = j.currenContext[:n] // Pop
}

func (j *jsonSecretStore) combine(values []string) string {
	return strings.Join(values, j.nestedSeparator)
}

func (j *jsonSecretStore) getJSONSecretStoreMetadata(spec secretstores.Metadata) (*jsonSecretStoreMetaData, error) {
	b, err := json.Marshal(spec.Properties)
	if err != nil {
		return nil, err
	}

	var meta jsonSecretStoreMetaData
	err = json.Unmarshal(b, &meta)
	if err != nil {
		return nil, err
	}
	if meta.SecretsFile == "" {
		return nil, fmt.Errorf("missing JSON secrets file in metadata")
	}
	return &meta, nil
}

func (j *jsonSecretStore) readJSONFile(secretsFile string) (map[string]interface{}, error) {
	j.secretsFile = secretsFile
	jsonFile, err := os.Open(secretsFile)
	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return nil, err
	}

	var jsonConfig map[string]interface{}
	err = json.Unmarshal(byteValue, &jsonConfig)
	if err != nil {
		return nil, err
	}

	return jsonConfig, nil
}
