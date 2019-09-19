package config

// DynamoDbConfig is the configuration for an AWS DynamoDB store.
type DynamoDbConfig struct {
	TableName string     `mapstructure:"table"`
	AWS       *AwsConfig `mapstructure:"aws"`
}

type AwsConfig struct {
	Endpoint *string `mapstructure:"endpoint"`
	Region   *string `mapstructure:"region"`
}
