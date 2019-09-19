# Grafeas - DynamoDb

This project provides a [Grafeas](https://github.com/grafeas/grafeas) implementation that supports using AWS DynamoDB as a storage mechanism.

## Building

**IMPORTANT:** This project will not build correctly as per these instructions until https://github.com/grafeas/grafeas/pull/383/ has been merged into the main project.

Build using the provided Makefile or via Docker.

```shell
# Either build via make
make build

# or docker
docker build --rm .
```

## Unit tests

Testing is performed against a Docker container that contains the AWS-provided image that starts a local DynamoDB instance, https://hub.docker.com/r/amazon/dynamodb-local.  This requires that you have Docker installed on your build server and that the Docker daemon is running.  The container is controlled through Golang.

```shell
make test
```

The container is started prior to the tests running and is stopped when the tests finish.  The container is called `dynamoDbTestingContainer` and by default runs on port `8000`.  If you have a service running on that port then this can be overridden by setting the environment variable `GRAFEAS_DYNAMODB_TEST_PORT` to an alternative port.  

## Configuring

The server looks for a configuration file that is passed in via the `--config` argument.  That file should be in YAML format and follows the specification laid down by the main Grafeas project.  There is an additional configuration namespace that must be set in order to use DynamoDB.

```yaml
grafeas:
  storage_type: dynamodb
  dynamodb:
    table: "Name_of_table_to_use_within_DynamoDB"
    aws:
      endpoint: "http://localhost:1234"
      region: "eu-west-1" 
  ...
```

The AWS configuration options are used for defining how to interact with DynamoDB.  They are optional.

| Option        | Meaning           | Example  |
| ------------- |-------------| -----|
| endpoint      | AWS endpoint.  Set if you wish to run against a local DynamoDB instance, otherwise leave blank or do not use. | `http://localhost:8000` |
| region      | AWS region.  Set if you wish to run against a DynamoDB instance in you non-default region, otherwise leave blank or do not use. | `eu-west-1` |

Configuration options are translated to their respective [AWS Config](https://docs.aws.amazon.com/sdk-for-go/api/aws/#Config) equivalent, where the name is uppercase in the Grafeas yaml config. 

The `...` in the snippet above refers to the any other configuration required by Grafeas.  A simple working example is below:
```yaml
grafeas:
  api:
    # Endpoint address
    address: "0.0.0.0:8080"
    # PKI configuration (optional)
    cafile: ca.crt
    keyfile: ca.key
    certfile: ca.crt
    # CORS configuration (optional)
    cors_allowed_origins:
      # - "http://example.net"
  storage_type: dynamodb
  dynamodb:
    table: "Name_of_table_to_use_within_DynamoDB"
```

This instance of Grafeas also supports all the storage mechanisms defined within the main Grafeas project.  Note that if `dynamodb` is not specified as the `storage_type`, then this instance of Grafeas will use the default storage mechanism (which is currently `memstore`).

The configuration file is specified by way of the `--config` argument
```shell
--config /path/to/config.yaml
```

## Running the Server

This implementation requires a DynamoDB instance to operate against.  That instance may be an AWS instance, or it can be a local instance.  If it is local, the following variables need to be set (to anything): `AWS_REGION`, `AWS_ACCESS_KEY`, `AWS_SECRET_ACCESS_KEY`.

If an AWS instance is the target, then credentials are parsed from the underlying system as per AWS's documentation.

Pass the name of a configuration file to the executable via the `--config` command line argument.    

```shell
cd go/v1beta1
go run main/main.go  -- --config /path/to/your/config.yaml
```

This will start the Grafeas gRPC and REST APIs on `localhost:8080`.

## DynamoDB Details

### Preamble

If you haven't used DynamoDB before, then these two resources are fantastic by way of an introduction to data modelling in NoSQL:

1. https://www.youtube.com/watch?v=HaEPXoXVf2k
2. https://www.trek10.com/blog/dynamodb-single-table-relational-modeling/

The AWS DynamoDB Developer Guide is [here](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html).

### Data Model

The Grafeas data is stored in a single table, as per AWS best practice.  That table name is customisable via configuration.  If the table does not exist when Grafeas is started, then the application will attempt to create it.

Data is stored using 4 columns, called `PartitionKey`, `SortKey`, `Data` and `Json`.  What data is actually stored in the columns depends on the item being stored.

There are 2 indices:

1. Global Primary Index (GPI):
   - **Hash**:  *PartitionKey*
   - **Range**: *SortKey*
2. Global Secondary Index (GSI1):
   - **Hash**:  *SortKey*
   - **Range**: *Data*


|  Data Object          | PartitionKey | SortKey | Data | Json |
| ------------- |-------------|-------------|-------------|-------------|
| Project | Project name (`projects/[PROJECT ID]`) | `"PROJECT"` | Project name (`projects/[PROJECT ID]`) | Json representation of Project |
| Note | Note name (`projects/[PROJECT ID]/notes/[NOTE ID]`) | `"NOTE"` | Project name (`projects/[PROJECT ID]`) | Json representation of Note |
| Occurrence | Occurrence name (`projects/[PROJECT_ID]/occurrences/[OCCURRENCE_ID]`) | `"OCCURRENCE"` | Project ID | Json representation of Occurrence |
| Occurrence note | Occurrence name (`projects/[PROJECT_ID]/occurrences/[OCCURRENCE_ID]`) | Note name (`projects/[NOTE PROJECT ID]/notes/[NOTE ID]`) | Occurrence name (`projects/[PROJECT_ID]/occurrences/[OCCURRENCE_ID]`) | Json representation of Occurrence |

Projects and Notes can be queried by ID using the GPI, or listing all items of that respective type by means of the GSI, in which case they will be lexicographically sorted.  It's important to realise that Notes and Occurrences may be stored in different projects (this is the recommendation within the Grafeas documentation).

Note that when Occurrences are created, 2 rows are created in the table (this is the Adjacency List pattern described in the 2 resources (blog and video) listed above).  The first row allows for querying by ID using the GPI, or listing all Occurrences by means of the GSI, as is the case for Projects and Notes.  The second row saves the associated Note name in the `Data` column, which means that the Note associated with a given Occurrence can be retrieved by means of the GPI (parsing the Note ID from the Occurrence, then querying the GPI for that Note ID).  Additionally, all occurrences across all projects associated with a given Note can be queried using the GSI (`SortKey` contains the Note name of interest). 

Pagination support is provided out of the box with DynamoDB; see the main Grafeas documentation for how to use this.

No support is currently provided for migration of schemas in the event of changes to the Grafeas structure and thus any c=such migrations will need to be performed manually.

## Contributing

Pull requests welcome.

## License

Grafeas-dynamodb is under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details.
