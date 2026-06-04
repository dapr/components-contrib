package aws

// DeprecatedPostgresIAM contains legacy aws IAM fields used by PostgreSQL
// components. This is retained here to avoid breaking existing metadata parsing logic.
//
// NOTE: this type is kept for backwards compatibility and may be removed in a
// future release at any time from 1.17 onwards once all consumers have migrated.
type DeprecatedPostgresIAM struct {
	// Access key to use for accessing PostgreSQL.
	AccessKey string `json:"awsAccessKey" mapstructure:"awsAccessKey"`
	// Secret key to use for accessing PostgreSQL.
	SecretKey string `json:"awsSecretKey" mapstructure:"awsSecretKey"`
}
