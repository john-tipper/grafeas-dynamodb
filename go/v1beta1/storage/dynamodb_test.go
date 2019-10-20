package storage_test

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/grafeas/grafeas/go/v1beta1/api"
	"github.com/grafeas/grafeas/go/v1beta1/project"
	gs "github.com/grafeas/grafeas/go/v1beta1/storage"
	"log"
	"os"
	"testing"

	"github.com/john-tipper/grafeas-dynamodb/go/config"
	"github.com/john-tipper/grafeas-dynamodb/go/v1beta1/storage"
)

const (
	dynamoDbTestingContainerPort = 8000
)

func dropDynamoDbTable(tableName string, dynamoDb *dynamodb.DynamoDB) {
	_, err := dynamoDb.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	},
	)
	if err != nil {
		log.Panicf("Unable to delete table %s", tableName)
	}
}

func TestDynamoDbStore(t *testing.T) {
	hostPort := dynamoDbTestingContainerPort
	createDynamoDbStore := func(t *testing.T) (grafeas.Storage, project.Storage, func()) {

		dynamoDbConfig := &config.DynamoDbConfig{
			TableName: "test_table",
			AWS: &config.AwsConfig{
				Endpoint: aws.String(fmt.Sprintf("http://localhost:%d", hostPort)),
			},
		}
		err := os.Setenv("AWS_REGION", "eu-west-2")
		if err != nil {
			log.Panic("Error setting AWS_REGION env variable")
		}
		err = os.Setenv("AWS_ACCESS_KEY", "TEST_ACCESS_KEY")
		if err != nil {
			log.Panic("Error setting AWS_ACCESS_KEY env variable")
		}
		err = os.Setenv("AWS_SECRET_ACCESS_KEY", "TEST_SECRET_ACCESS_KEY")
		if err != nil {
			log.Panic("Error setting AWS_SECRET_ACCESS_KEY env variable")
		}

		ddb := storage.NewDynamoDbStore(dynamoDbConfig)
		var g grafeas.Storage = ddb
		var gp project.Storage = ddb
		return g, gp, func() { dropDynamoDbTable(dynamoDbConfig.TableName, ddb.DynamoDB) }
	}

	gs.DoTestStorage(t, createDynamoDbStore)
}
