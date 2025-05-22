package postgresql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildConnectionString(t *testing.T) {
	tc := []struct {
		name        string
		metadata    PostgresAuthMetadata
		expected    string
		expectError bool
	}{
		{
			name:        "empty metadata",
			metadata:    PostgresAuthMetadata{},
			expected:    "",
			expectError: true,
		},
		{
			name: "valid DSN connection string",
			metadata: PostgresAuthMetadata{
				ConnectionString: "host=localhost user=postgres password=example port=5432 connect_timeout=10 database=my_db",
			},
			expected:    "host=localhost user=postgres password=example port=5432 connect_timeout=10 database=my_db",
			expectError: false,
		},
		{
			name: "invalid DSN string with metadata",
			metadata: PostgresAuthMetadata{
				ConnectionString: "this is not a valid connection string",
			},
			expected:    "",
			expectError: true,
		},
		{
			name: "DSN string overridden by metadata",
			metadata: PostgresAuthMetadata{
				ConnectionString: "host=localhost user=postgres password=example",
				User:             "overrideUser",
				Password:         "overridePass",
			},
			expected:    "host=localhost user=overrideUser password=overridePass",
			expectError: false,
		},
		{
			name: "DSN missing user, added by metadata",
			metadata: PostgresAuthMetadata{
				ConnectionString: "host=localhost port=5432",
				User:             "fromMetadata",
			},
			expected:    "host=localhost port=5432 user=fromMetadata",
			expectError: false,
		},
		{
			name: "URL connection string no metadata",
			metadata: PostgresAuthMetadata{
				ConnectionString: "postgres://user:pass@localhost:5432/mydb?sslmode=disable",
			},
			expected:    "postgres://user:pass@localhost:5432/mydb?sslmode=disable",
			expectError: false,
		},
		{
			name: "URL connection string overridden by metadata",
			metadata: PostgresAuthMetadata{
				ConnectionString: "postgres://original:secret@localhost:5432/mydb?sslmode=disable",
				User:             "newuser",
				Password:         "newpass",
				Host:             "newhost",
				Port:             "5433",
				Database:         "newdb",
				SslRootCert:      "/certs/root.pem",
			},
			expected:    "postgres://newuser:newpass@newhost:5433/newdb?sslmode=disable&sslrootcert=%2Fcerts%2Froot.pem",
			expectError: false,
		},
		{
			name: "URL connection string overridden by metadata with hostaddr",
			metadata: PostgresAuthMetadata{
				ConnectionString: "postgres://original:secret@localhost:5432/mydb?sslmode=disable",
				User:             "newuser",
				Password:         "newpass",
				Host:             "newhost",
				HostAddr:         "127.0.0.1",
				Port:             "5433",
				Database:         "newdb",
				SslRootCert:      "/certs/root.pem",
			},
			expected:    "postgres://newuser:newpass@127.0.0.1:5433/newdb?sslmode=disable&sslrootcert=%2Fcerts%2Froot.pem",
			expectError: false,
		},
		{
			name: "URL connection string adds sslrootcert via metadata",
			metadata: PostgresAuthMetadata{
				ConnectionString: "postgres://user:pass@localhost:5432/mydb?sslmode=verify-full",
				SslRootCert:      "/certs/root.pem",
			},
			expected:    "postgres://user:pass@localhost:5432/mydb?sslmode=verify-full&sslrootcert=%2Fcerts%2Froot.pem",
			expectError: false,
		},
		{
			name: "URL connection string adds sslrootcert via metadata with hostaddr",
			metadata: PostgresAuthMetadata{
				ConnectionString: "postgres://user:pass@localhost:5432/mydb?sslmode=verify-full",
				SslRootCert:      "/certs/root.pem",
				HostAddr:         "127.0.0.1",
				Port:             "5433",
			},
			expected:    "postgres://user:pass@127.0.0.1:5433/mydb?sslmode=verify-full&sslrootcert=%2Fcerts%2Froot.pem",
			expectError: false,
		},
	}
	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := tt.metadata.buildConnectionString()
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, actual)
			}
		})
	}
}

func TestURLBuildConnectionStringOnlyWithMetadata(t *testing.T) {
	metadata := PostgresAuthMetadata{
		ConnectionString: "postgres://original:secret@localhost:5432/mydb?sslmode=disable",
		User:             "postgres",
		Password:         "example",
		Host:             "localhost",
		Port:             "5432",
		Database:         "my_db",
		SslRootCert:      "/certs/root.pem",
	}
	actual, err := metadata.buildConnectionString()
	require.NoError(t, err)
	require.Equal(t, "postgres://postgres:example@localhost:5432/my_db?sslmode=disable&sslrootcert=%2Fcerts%2Froot.pem", actual)
}

func TestDSNBuildConnectionStringOnlyWithMetadata(t *testing.T) {
	metadata := PostgresAuthMetadata{
		User:        "postgres",
		Password:    "example",
		Host:        "localhost",
		Port:        "5432",
		Database:    "my_db",
		SslRootCert: "/certs/root.pem",
	}
	actual, err := metadata.buildConnectionString()
	require.NoError(t, err)
	parts := strings.Split(actual, " ")
	for _, part := range parts {
		kv := strings.Split(part, "=")
		require.Len(t, kv, 2)
		switch kv[0] {
		case "host":
			require.Equal(t, "localhost", kv[1])
		case "user":
			require.Equal(t, "postgres", kv[1])
		case "password":
			require.Equal(t, "example", kv[1])
		case "port":
			require.Equal(t, "5432", kv[1])
		case "database":
			require.Equal(t, "my_db", kv[1])
		case "sslrootcert":
			require.Equal(t, "/certs/root.pem", kv[1])
		}
	}
}
