package storage_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/docker/docker/api/types/filters"
	"github.com/grafeas/grafeas/go/v1beta1/api"
	"github.com/grafeas/grafeas/go/v1beta1/project"
	gs "github.com/grafeas/grafeas/go/v1beta1/storage"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/john-tipper/grafeas-dynamodb/go/config"
	"github.com/john-tipper/grafeas-dynamodb/go/v1beta1/storage"
)

const (
	dynamoDbTestingContainer     = "dynamoDbTestingContainer"
	dynamoDbTestingContainerPort = "GRAFEAS_DYNAMODB_TEST_PORT"
)

func CreateDockerContainer(image string, port string, hostPort string) (string, error) {
	hostIp := "127.0.0.1"
	docker, err := client.NewClientWithOpts(client.WithAPIVersionNegotiation())
	if err != nil {
		log.Panicf("Unable to create docker client: %s", err)
	}

	// stop existing container if required
	existingContainerId := GetPreExistingTestContainerId(dynamoDbTestingContainer)
	if existingContainerId != "" {
		err := StopDockerContainer(existingContainerId)
		if err != nil {
			log.Printf("Error stopping existing testing container:\n%s", err)
		}
		err = docker.ContainerRemove(context.Background(), existingContainerId, types.ContainerRemoveOptions{})
		if err != nil {
			log.Printf("Error removing existing testing container:\n%s", err)
		}
	}

	hostBinding := nat.PortBinding{
		HostIP:   hostIp,
		HostPort: hostPort,
	}
	containerPort, err := nat.NewPort("tcp", port)
	if err != nil {
		log.Panicf("Unable to get container port %s\n%s", port, err)
	}

	portBinding := nat.PortMap{containerPort: []nat.PortBinding{hostBinding}}
	cont, err := docker.ContainerCreate(
		context.Background(),
		&container.Config{
			Image: image,
		},
		&container.HostConfig{
			PortBindings: portBinding,
		},
		nil,
		dynamoDbTestingContainer,
	)
	if err != nil {
		log.Panicf("Unable to create container %s:%s binding to %s:%s\n%s", image, port, hostIp, hostPort, err)
	}

	err = docker.ContainerStart(context.Background(), cont.ID, types.ContainerStartOptions{})
	if err != nil {
		log.Panic("Unable to start container, exiting")
	}
	log.Printf("Container %s is started", cont.ID)
	const sleepTime = "5s"
	log.Printf("Sleeping %s whilst container starts...", sleepTime)
	duration, _ := time.ParseDuration(sleepTime)
	time.Sleep(duration)
	log.Print("Done sleeping.")
	return cont.ID, nil
}

func GetPreExistingTestContainerId(containerName string) string {
	docker, err := client.NewClientWithOpts(client.WithAPIVersionNegotiation())
	if err != nil {
		panic(err)
	}
	containerId := ""
	filt := filters.NewArgs()
	filt.Add("name", containerName)

	containers, err := docker.ContainerList(context.Background(), types.ContainerListOptions{
		Filters: filt,
		All:     true,
	})

	if err != nil {
		return containerId
	}

	if len(containers) > 0 {
		containerId = containers[0].ID
	}

	return containerId
}

func StopDockerContainer(containerID string) error {
	docker, err := client.NewClientWithOpts(client.WithAPIVersionNegotiation())
	if err != nil {
		panic(err)
	}

	err = docker.ContainerStop(context.Background(), containerID, nil)
	if err != nil {
		panic(err)
	}
	return err
}

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
	log.Print("Creating container...")
	hostPort := os.Getenv(dynamoDbTestingContainerPort)
	if hostPort == "" {
		hostPort = "8000"
	}
	containerId, err := CreateDockerContainer("amazon/dynamodb-local", "8000", hostPort)
	if err != nil {
		log.Panic("Error creating DynamoDb local testing container")
	}
	defer func() {
		log.Printf("Stopping container %s", containerId)
		err := StopDockerContainer(containerId)
		if err != nil {
			log.Panicf("Error stopping container %s", containerId)
		}
	}()
	createDynamoDbStore := func(t *testing.T) (grafeas.Storage, project.Storage, func()) {

		dynamoDbConfig := &config.DynamoDbConfig{
			TableName: "test_table",
			AWS: &config.AwsConfig{
				Endpoint: aws.String(fmt.Sprintf("http://localhost:%s", hostPort)),
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
