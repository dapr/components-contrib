package authentication_test

import (
	"strings"
	"testing"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/components-contrib/pubsub/jetstream"
	"github.com/dapr/kit/logger"
)

func TestAuthentication(t *testing.T) {
	testCases := []struct {
		desc        string
		jwt         string
		seedKey     string
		expectError bool
	}{
		{
			desc:        "Valid jwt and valid seed",
			jwt:         "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJWS1UySENWNUlBRzUzSkNQT0hRNllaTDJKSEZZVkJBVzJSQ1RMUUtBWTJLT0UySDZSSDdBIiwiaWF0IjoxNjQyNDkwNjU5LCJpc3MiOiJBRFU0UVVNV0RQMkJKVFBIWktFWkNLVkw3STNIQTRPR1dRUUNWMjJRVlBHSVNTV1Q0T1hRN0VOUyIsIm5hbWUiOiJsb3Zpbmdfa2VsbGVyIiwic3ViIjoiVUFUSkNBWFRCNUoyRVVWNDJCU0UzMkRBSEpKS01WTkZNNlNIUkZXWDVUWkVMWDZEU1hURVlXVTUiLCJuYXRzIjp7InB1YiI6e30sInN1YiI6e30sInN1YnMiOi0xLCJkYXRhIjotMSwicGF5bG9hZCI6LTEsInR5cGUiOiJ1c2VyIiwidmVyc2lvbiI6Mn19.7K5VA23V8bBipH4Vkzhg01_jaQVw6MWW9NgxUkiYBUaMLNHLZB4_DnuT2mBz9SrQwi8XohNoZeKI7WTTbQxxBg",
			seedKey:     "SUAJF7ALJKFPLKTA23BOP6CILVPH3SV4V7EGM7VFZ3KCFWHKPPI7HCCZKI",
			expectError: false,
		},
		{
			desc:        "Valid jwt and invalid seed",
			jwt:         "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJWS1UySENWNUlBRzUzSkNQT0hRNllaTDJKSEZZVkJBVzJSQ1RMUUtBWTJLT0UySDZSSDdBIiwiaWF0IjoxNjQyNDkwNjU5LCJpc3MiOiJBRFU0UVVNV0RQMkJKVFBIWktFWkNLVkw3STNIQTRPR1dRUUNWMjJRVlBHSVNTV1Q0T1hRN0VOUyIsIm5hbWUiOiJsb3Zpbmdfa2VsbGVyIiwic3ViIjoiVUFUSkNBWFRCNUoyRVVWNDJCU0UzMkRBSEpKS01WTkZNNlNIUkZXWDVUWkVMWDZEU1hURVlXVTUiLCJuYXRzIjp7InB1YiI6e30sInN1YiI6e30sInN1YnMiOi0xLCJkYXRhIjotMSwicGF5bG9hZCI6LTEsInR5cGUiOiJ1c2VyIiwidmVyc2lvbiI6Mn19.7K5VA23V8bBipH4Vkzhg01_jaQVw6MWW9NgxUkiYBUaMLNHLZB4_DnuT2mBz9SrQwi8XohNoZeKI7WTTbQxxBg",
			seedKey:     "SUAIGUGTLK7FZMRVYLGCROG3TTCZK64D2MZOYFGKH7WJRADFYOMTL3QQZU",
			expectError: true,
		},
		{
			desc:        "Invalid jwt and valid seed",
			jwt:         "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiI0SlJGU0RHTk9JM1BDUTIyTVhLMk1SNkhTSk1SREhZTlNDSlNaUDcyVzJUQ1g2Sk9SWDdBIiwiaWF0IjoxNjQyNDkwNjU5LCJpc3MiOiJBQzc0QllIS1VEUzVLUEg0TDRZVkNVRUNEVDJIUFQzRDVFU1pXUVNDTDNMVFhaRkhEVEg2T1ZSVyIsIm5hbWUiOiJzeXMiLCJzdWIiOiJVQk03WEpHRU5VUUtQQVY2RE9XUjJaWDdXM0ZQUEJVUE9DQlZRNzU2UFFaUUg2VlVGRjdORFNTNSIsIm5hdHMiOnsicHViIjp7fSwic3ViIjp7fSwic3VicyI6LTEsImRhdGEiOi0xLCJwYXlsb2FkIjotMSwiaXNzdWVyX2FjY291bnQiOiJBQ0dDV1ZZWjYySTZSWkRRNlVVU0FEM0lTTE1FMkNFUEdOWlczUVpCN09KUkE1VU5LMkRDT08zQSIsInR5cGUiOiJ1c2VyIiwidmVyc2lvbiI6Mn19.Cj_dQFE9c_BDZrHLzIJRtDI0ZrXB1u-eG_NJw54Dg3Df3QdJOrl-rU_M6f0D0rAGH1THQl0vD4lRkGp_SfzeAQ",
			seedKey:     "SUAJF7ALJKFPLKTA23BOP6CILVPH3SV4V7EGM7VFZ3KCFWHKPPI7HCCZKI",
			expectError: true,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			log := logger.NewLogger("natsE2E")
			js := jetstream.NewJetStream(log)
			md := pubsub.Metadata{
				Properties: map[string]string{
					"natsURL":        "nats://localhost",
					"jwt":            tC.jwt,
					"seedKey":        tC.seedKey,
					"durableName":    "test",
					"queueGroupName": "test",
					"startSequence":  "0",
					"startTime":      "0",
					"deliverAll":     "true",
					"flowControl":    "false",
				},
			}
			err := js.Init(md)
			if err != nil && !tC.expectError {
				if strings.Contains(err.Error(), "network is unreachable") {
					log.Warn("jetstream server is unreachable")
					return
				}
				t.Fatal("Did not expect error during connect", err.Error())
			}
			if err == nil && tC.expectError {
				t.Fatal("Did expect error during connect")
			}
		})
	}
}
