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

package internal

import "testing"

func TestGetRedisValueAndVersion(t *testing.T) {
	type args struct {
		redisValue string
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 string
	}{
		{
			name: "empty value",
			args: args{
				redisValue: "",
			},
			want:  "",
			want1: "",
		},
		{
			name: "value without version",
			args: args{
				redisValue: "mockValue",
			},
			want:  "mockValue",
			want1: "",
		},
		{
			name: "value without version",
			args: args{
				redisValue: "mockValue||",
			},
			want:  "mockValue",
			want1: "",
		},
		{
			name: "value with version",
			args: args{
				redisValue: "mockValue||v1.0.0",
			},
			want:  "mockValue",
			want1: "v1.0.0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := GetRedisValueAndVersion(tt.args.redisValue)
			if got != tt.want {
				t.Errorf("GetRedisValueAndVersion() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetRedisValueAndVersion() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestParseRedisKeyFromChannel(t *testing.T) {
	type args struct {
		eventChannel string
		redisDB      int
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "invalid channel name",
			args: args{
				eventChannel: "invalid channel name",
				redisDB:      0,
			},
			want:    "",
			wantErr: true,
		}, {
			name: "valid channel name with DB 0",
			args: args{
				eventChannel: keySpacePrefix + "0__:key",
				redisDB:      0,
			},
			want:    "key",
			wantErr: false,
		}, {
			name: "valid channel name with DB 1",
			args: args{
				eventChannel: keySpacePrefix + "1__:key",
				redisDB:      1,
			},
			want:    "key",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseRedisKeyFromChannel(tt.args.eventChannel, tt.args.redisDB)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseRedisKeyFromChannel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseRedisKeyFromChannel() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetRedisChannelFromKey(t *testing.T) {
	type args struct {
		key     string
		redisDB int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "key with redisDB 0",
			args: args{
				key:     "key",
				redisDB: 0,
			},
			want: keySpacePrefix + "0__:key",
		}, {
			name: "key with redisDB 1",
			args: args{
				key:     "key",
				redisDB: 1,
			},
			want: keySpacePrefix + "1__:key",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetRedisChannelFromKey(tt.args.key, tt.args.redisDB)
			if got != tt.want {
				t.Errorf("GetRedisChannelFromKey() got = %v, want %v", got, tt.want)
			}
		})
	}
}
