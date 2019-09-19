package config_test

import (
	"github.com/aws/aws-sdk-go/aws"
	grafeasConfig "github.com/grafeas/grafeas/go/config"
	"github.com/john-tipper/grafeas-dynamodb/go/config"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

var dynamodbconfig_yaml = []byte(`
grafeas:
  api:
    address: "0.0.0.0:8081"
    certfile: abc
    keyfile: def
    cafile:  ghi
    cors_allowed_origins:
      - "http://example.com"
      - "https://somewhere.else.com"
  storage_type: "dynamodb"
  dynamodb:
    table: "Name_of_table_to_use_within_DynamoDB"
    aws:
      endpoint: "http://localhost:1234"
      region: "eu-west-1"
`)

func TestAWSConfigParsesOk(t *testing.T) {
	file, err := ioutil.TempFile("", "config.*.yaml")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = os.Remove(file.Name())
	}()

	_, err = file.Write(dynamodbconfig_yaml)
	if err != nil {
		log.Fatal(err)
	}

	err = file.Close()
	if err != nil {
		log.Fatal(err)
	}

	cfg, err := grafeasConfig.LoadConfig(file.Name())
	if err != nil {
		t.Error(err)
	}

	dynamodbConfig := config.DynamoDbConfig{}

	err = grafeasConfig.ConvertGenericConfigToSpecificType(*cfg.StorageConfig, &dynamodbConfig)
	if err != nil {
		t.Errorf("Unable to create DynamoDB from parsed configuration file, %s", err)
	}

	awsConfig := aws.Config{}
	err = grafeasConfig.ConvertGenericConfigToSpecificType(dynamodbConfig.AWS, &awsConfig)
	if err != nil {
		t.Errorf("Unable to create AWS Config from parsed configuration, %s", err)
	}

	if *awsConfig.Endpoint != "http://localhost:1234" {
		t.Errorf("Endpoint is incorrect, got '%s', expected 'http://localhost:1234'", *awsConfig.Endpoint)
	}

	if *awsConfig.Region != "eu-west-1" {
		t.Errorf("Region is incorrect, got '%s', expected 'eu-west-1'", *awsConfig.Region)
	}

}
