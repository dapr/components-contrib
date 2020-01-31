package twilio

import (
	"errors"
	"fmt"
	"github.com/dapr/components-contrib/bindings"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	toNumber      = "toNumber"
	fromNumber    = "fromNumber"
	accountSid    = "accountSid"
	authToken     = "authToken"
	timeout       = "timeout"
	twilioURLBase = "https://api.twilio.com/2010-04-01/Accounts/"
)

type OutputBinding interface {
	Init(metadata bindings.Metadata) error
	Write(req *bindings.WriteRequest) error
}

type SMS struct {
	metadata twilioMetadata
}

type twilioMetadata struct {
	toNumber   string
	fromNumber string
	accountSid string
	authToken  string
	timeout    time.Duration
}

func NewSMS() *SMS {
	return &SMS{}
}

func (t *SMS) Init(metadata bindings.Metadata) error {
	twilioM := twilioMetadata{
		timeout: time.Minute * 5,
	}

	if metadata.Properties[toNumber] == "" {
		return errors.New("\"to\" is a required field")
	}
	if metadata.Properties[fromNumber] == "" {
		return errors.New("\"fromNumber\" is a required field")
	}
	if metadata.Properties[accountSid] == "" {
		return errors.New("\"accountSid\" is a required field")
	}
	if metadata.Properties[authToken] == "" {
		return errors.New("\"authToken\" is a required field")
	}

	twilioM.toNumber = metadata.Properties[toNumber]
	twilioM.fromNumber = metadata.Properties[fromNumber]
	twilioM.accountSid = metadata.Properties[accountSid]
	twilioM.authToken = metadata.Properties[authToken]
	if metadata.Properties[timeout] != "" {
		t, err := time.ParseDuration(metadata.Properties[timeout])
		if err != nil {
			return fmt.Errorf("error parsing timeout: %s", err)
		}
		twilioM.timeout = t
	}

	t.metadata = twilioM
	return nil
}

func (t *SMS) Write(req *bindings.WriteRequest) error {
	v := url.Values{}
	v.Set("To", t.metadata.toNumber)
	v.Set("From", t.metadata.fromNumber)
	v.Set("Body", string(req.Data))
	vDr := *strings.NewReader(v.Encode())

	client := &http.Client{
		Timeout: t.metadata.timeout,
	}

	twilioURL := fmt.Sprintf("%s%s/Messages.json", twilioURLBase, t.metadata.accountSid)
	httpReq, err := http.NewRequest("POST", twilioURL, &vDr)
	if err != nil {
		return err
	}
	httpReq.SetBasicAuth(t.metadata.accountSid, t.metadata.authToken)
	httpReq.Header.Add("Accept", "application/json")
	httpReq.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		return fmt.Errorf("error from Twilio: %s", resp.Status)
	}
	return nil
}
