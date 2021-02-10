// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package file

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

type localSecretStoreMetaData struct {
	SecretsFile     string `json:"secretsFile"`
	NestedSeparator string `json:"nestedSeparator"`
}

type localSecretStore struct {
	secretsFile     string
	nestedSeparator string
	currenContext   []string
	currentPath     string
	secrets         map[string]string
	readLocalFileFn func(secretsFile string) (map[string]interface{}, error)
	logger          logger.Logger
}

// NewLocalSecretStore returns a new Local secret store
func NewLocalSecretStore(logger logger.Logger) secretstores.SecretStore {
	return &localSecretStore{
		logger: logger,
	}
}

// Init creates a Local secret store
func (j *localSecretStore) Init(metadata secretstores.Metadata) error {
	meta, err := j.getLocalSecretStoreMetadata(metadata)
	if err != nil {
		return err
	}

	if len(meta.NestedSeparator) == 0 {
		j.nestedSeparator = ":"
	} else {
		j.nestedSeparator = meta.NestedSeparator
	}

	if j.readLocalFileFn == nil {
		j.readLocalFileFn = j.readLocalFile
	}

	j.secrets = map[string]string{}

	jsonConfig, err := j.readLocalFileFn(meta.SecretsFile)
	if err != nil {
		return err
	}

	j.visitJSONObject(jsonConfig)

	return nil
}

// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values
func (j *localSecretStore) GetSecret(req secretstores.GetSecretRequest) (secretstores.GetSecretResponse, error) {
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

// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values
func (j *localSecretStore) BulkGetSecret(req secretstores.BulkGetSecretRequest) (secretstores.BulkGetSecretResponse, error) {
	r := map[string]map[string]string{}

	for k, v := range j.secrets {
		r[k] = map[string]string{k: v}
	}

	return secretstores.BulkGetSecretResponse{
		Data: r,
	}, nil
}

func (j *localSecretStore) visitJSONObject(jsonConfig map[string]interface{}) error {
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

func (j *localSecretStore) enterContext(context string) {
	j.currenContext = append(j.currenContext, context)
	j.currentPath = j.combine(j.currenContext)
}

func (j *localSecretStore) visitPrimitive(context string) error {
	key := j.currentPath
	_, exists := j.secrets[key]

	if exists {
		return errors.New("duplicate key")
	}

	j.secrets[key] = context

	return nil
}

func (j *localSecretStore) visitArray(array []interface{}) error {
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

func (j *localSecretStore) visitProperty(property interface{}) error {
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

func (j *localSecretStore) exitContext() {
	j.pop()
	j.currentPath = j.combine(j.currenContext)
}

func (j *localSecretStore) pop() {
	n := len(j.currenContext) - 1 // Top element
	j.currenContext[n] = ""
	j.currenContext = j.currenContext[:n] // Pop
}

func (j *localSecretStore) combine(values []string) string {
	return strings.Join(values, j.nestedSeparator)
}

func (j *localSecretStore) getLocalSecretStoreMetadata(spec secretstores.Metadata) (*localSecretStoreMetaData, error) {
	b, err := json.Marshal(spec.Properties)
	if err != nil {
		return nil, err
	}

	var meta localSecretStoreMetaData
	err = json.Unmarshal(b, &meta)
	if err != nil {
		return nil, err
	}
	if meta.SecretsFile == "" {
		return nil, fmt.Errorf("missing local secrets file in metadata")
	}

	return &meta, nil
}

func (j *localSecretStore) readLocalFile(secretsFile string) (map[string]interface{}, error) {
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
