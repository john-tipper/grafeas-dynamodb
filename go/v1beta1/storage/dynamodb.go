package storage

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	grafeasConfig "github.com/grafeas/grafeas/go/config"
	"github.com/grafeas/grafeas/go/name"
	"github.com/grafeas/grafeas/go/v1beta1/storage"
	pb "github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	prpb "github.com/grafeas/grafeas/proto/v1beta1/project_go_proto"
	"github.com/john-tipper/grafeas-dynamodb/go/config"
	"golang.org/x/net/context"
	fieldmaskpb "google.golang.org/genproto/protobuf/field_mask"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DynamoDb struct {
	*dynamodb.DynamoDB
	TableName string
}

func DynamodbStorageTypeProvider(storageType string, storageConfig *grafeasConfig.StorageConfiguration) (*storage.Storage, error) {
	if storageType != "dynamodb" {
		return nil, errors.New(fmt.Sprintf("Unknown storage type %s, must be 'dynamodb'", storageType))
	}

	var storeConfig config.DynamoDbConfig

	err := grafeasConfig.ConvertGenericConfigToSpecificType(storageConfig, &storeConfig)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Unable to create DynamoDbConfig, %s", err))
	}

	s := NewDynamoDbStore(&storeConfig)
	storage := &storage.Storage{
		Ps: s,
		Gs: s,
	}

	return storage, nil
}

func NewDynamoDbStore(config *config.DynamoDbConfig) *DynamoDb {
	awsConfig := aws.Config{}

	err := grafeasConfig.ConvertGenericConfigToSpecificType(config.AWS, &awsConfig)
	if err != nil {
		log.Panicf("Unable to create AWS Config from configuration file, %s", err)
	}

	options := &session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config:            awsConfig,
	}

	sess := session.Must(session.NewSessionWithOptions(*options))

	dynamoDb := dynamodb.New(sess)
	if dynamoDb == nil {
		log.Panic("Could not create DynamoDB session")
	}

	err = createDynamoDbTables(dynamoDb, config)
	if err != nil {
		log.Panic(err.Error())
	}

	return &DynamoDb{
		DynamoDB:  dynamoDb,
		TableName: config.TableName,
	}
}

type DataItem struct {
	PartitionKey string
	SortKey      string
	Data         string
	NoteName     string
	Json         string
}

const (
	// constants relating to table structure
	GlobalSecondaryIndex1 = "GSI_1"
	PartitionKeyName      = "PartitionKey"
	SortKeyName           = "SortKey"
	DataKeyName           = "Data"
	JsonKeyName           = "Json"
	PaginationString      = "&"

	// constants relating to table contents
	projectSK    = "PROJECT"
	occurrenceSK = "OCCURRENCE"
	noteSK       = "NOTE"
)

func createDynamoDbTables(dynamoDb *dynamodb.DynamoDB, config *config.DynamoDbConfig) error {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(PartitionKeyName),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String(SortKeyName),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String(DataKeyName),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String(JsonKeyName),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(PartitionKeyName),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String(SortKeyName),
				KeyType:       aws.String("RANGE"),
			},
		},
		GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndex{
			{
				IndexName: aws.String(GlobalSecondaryIndex1),
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String(SortKeyName),
						KeyType:       aws.String("HASH"),
					},
					{
						AttributeName: aws.String(DataKeyName),
						KeyType:       aws.String("RANGE"),
					},
				},
				Projection: &dynamodb.Projection{
					ProjectionType: aws.String(dynamodb.ProjectionTypeAll),
				},
			},
		},
		BillingMode: aws.String(dynamodb.BillingModePayPerRequest),
		TableName:   aws.String(config.TableName),
	}
	_, err := dynamoDb.CreateTable(input)
	if err != nil {
		return err
	}
	return nil
}

// CreateProject creates the specified project in the storage.
func (db *DynamoDb) CreateProject(ctx context.Context, pID string, p *prpb.Project) (*prpb.Project, error) {
	m := jsonpb.Marshaler{}
	jsonObject, err := m.MarshalToString(p)
	if err != nil {
		log.Println("Unable to marshal project into json", err)
		return nil, status.Error(codes.Internal, "Unable to marshal project into json")
	}

	// use Global Primary Index for find by ID
	// use GSI for find all by type (PROJECT), sorted by name (Data)
	dataItem := DataItem{
		PartitionKey: name.FormatProject(pID),
		SortKey:      projectSK,
		Data:         name.FormatProject(pID),
		Json:         jsonObject,
	}

	av, err := dynamodbattribute.MarshalMap(dataItem)
	if err != nil {
		log.Println("Failed to marshal project into AttributeValues", err)
		return nil, status.Error(codes.Internal, "Failed to marshal project into AttributeValues")
	}

	input := &dynamodb.PutItemInput{
		Item:                av,
		TableName:           aws.String(db.TableName),
		ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s) AND attribute_not_exists(%s)", PartitionKeyName, SortKeyName)),
	}

	_, err = db.PutItem(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "ConditionalCheckFailedException" {
			return nil, status.Errorf(codes.AlreadyExists, "Project with name %q already exists", pID)
		} else {
			log.Println("Failed to insert Project in database", err)
			return nil, status.Error(codes.Internal, "Failed to insert Project in database")
		}
	}

	return p, nil
}

// GetProject gets the specified project from the storage.
func (db *DynamoDb) GetProject(ctx context.Context, pID string) (*prpb.Project, error) {

	result, err := db.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(db.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			PartitionKeyName: {
				S: aws.String(name.FormatProject(pID)),
			},
			SortKeyName: {
				S: aws.String(projectSK),
			},
		},
		ConsistentRead: aws.Bool(true),
	})

	if err != nil {
		log.Printf("Error when seeking project %s, %s", pID, err)
		return nil, nil
	}

	dataItem := DataItem{}
	err = dynamodbattribute.UnmarshalMap(result.Item, &dataItem)
	if err != nil {
		log.Panicf("Failed to unmarshal item, %v", err)
	}

	if dataItem.PartitionKey == "" {
		return nil, status.Errorf(codes.NotFound, "Project with name %q does not exist", pID)
	}

	var project prpb.Project
	err = jsonpb.Unmarshal(strings.NewReader(dataItem.Json), &project)
	if err != nil {
		log.Panicf("Failed to unmarshal json, %v", err)
	}

	return &project, nil

}

// ListProjects returns projects in the storage.
func (db *DynamoDb) ListProjects(ctx context.Context, filter string, pageSize int, pageToken string) ([]*prpb.Project, string, error) {
	var projects []*prpb.Project

	queryInput := dynamodb.QueryInput{
		TableName: aws.String(db.TableName),
		IndexName: aws.String(GlobalSecondaryIndex1),
		ExpressionAttributeNames: map[string]*string{
			"#PARTITION_KEY": aws.String(SortKeyName),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":PROJECT": {
				S: aws.String(projectSK),
			},
		},
		KeyConditionExpression: aws.String("#PARTITION_KEY=:PROJECT"),
		Limit:                  aws.Int64(int64(pageSize)),
	}

	if pageToken != "" {
		tokenArray := strings.Split(pageToken, PaginationString)
		if len(tokenArray) != 3 {
			log.Print("Error when trying to parse page token")
			return projects, "", nil
		}

		queryInput.ExclusiveStartKey = map[string]*dynamodb.AttributeValue{
			PartitionKeyName: {
				S: aws.String(tokenArray[0]),
			},
			SortKeyName: {
				S: aws.String(tokenArray[1]),
			},
			DataKeyName: {
				S: aws.String(tokenArray[2]),
			},
		}
	}

	result, err := db.Query(&queryInput)

	if err != nil {
		log.Printf("Error when listing projects:\n%s", err)
		return projects, "", nil
	}

	for _, item := range result.Items {
		dataItem := DataItem{}
		err = dynamodbattribute.UnmarshalMap(item, &dataItem)
		if err != nil {
			log.Panicf("Failed to unmarshal item, %v", err)
		}

		var project prpb.Project
		err = jsonpb.Unmarshal(strings.NewReader(dataItem.Json), &project)
		if err != nil {
			log.Panicf("Failed to unmarshal json, %v", err)
		}
		projects = append(projects, &project)
	}

	var token = ""
	if result.LastEvaluatedKey != nil {
		pk := *result.LastEvaluatedKey[PartitionKeyName].S
		sk := *result.LastEvaluatedKey[SortKeyName].S
		dk := *result.LastEvaluatedKey[DataKeyName].S
		if pk == "" || sk == "" || dk == "" {
			log.Panicf("Unable to unmarshal LastEvaluatedKey")
		} else {
			token = fmt.Sprintf("%s%s%s%s%s", pk, PaginationString, sk, PaginationString, dk)
		}
	}

	return projects, token, nil
}

// DeleteProject deletes the specified project from the storage.
func (db *DynamoDb) DeleteProject(ctx context.Context, pID string) error {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			PartitionKeyName: {
				S: aws.String(name.FormatProject(pID)),
			},
			SortKeyName: {
				S: aws.String(projectSK),
			},
		},
		TableName:           aws.String(db.TableName),
		ConditionExpression: aws.String(fmt.Sprintf("attribute_exists(%s) AND attribute_exists(%s)", PartitionKeyName, SortKeyName)),
	}

	_, err := db.DeleteItem(input)
	if err != nil {
		return err
	}

	return nil
}

// GetOccurrence gets the specified occurrence from storage.
func (db *DynamoDb) GetOccurrence(ctx context.Context, projectId, occId string) (*pb.Occurrence, error) {
	oName := name.FormatOccurrence(projectId, occId)
	result, err := db.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(db.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			PartitionKeyName: {
				S: aws.String(oName),
			},
			SortKeyName: {
				S: aws.String(occurrenceSK),
			},
		},
		ConsistentRead: aws.Bool(true),
	})

	if err != nil {
		log.Printf("Error when seeking occurrence %s, %s", oName, err)
		return nil, nil
	}

	dataItem := DataItem{}
	err = dynamodbattribute.UnmarshalMap(result.Item, &dataItem)
	if err != nil {
		log.Panicf("Failed to unmarshal item, %v", err)
	}

	if dataItem.PartitionKey == "" {
		return nil, status.Errorf(codes.NotFound, "Occurrence with name %s does not exist", oName)
	}

	var occurrence pb.Occurrence
	err = jsonpb.Unmarshal(strings.NewReader(dataItem.Json), &occurrence)
	if err != nil {
		log.Panicf("Failed to unmarshal json, %v\n%s", err, dataItem.Json)
	}

	return &occurrence, nil

}

// ListOccurrences lists occurrences for the specified project from storage.
func (db *DynamoDb) ListOccurrences(ctx context.Context, projectId, filter, pageToken string, pageSize int32) ([]*pb.Occurrence, string, error) {
	var occurrences []*pb.Occurrence

	queryInput := dynamodb.QueryInput{
		TableName: aws.String(db.TableName),
		IndexName: aws.String(GlobalSecondaryIndex1),
		ExpressionAttributeNames: map[string]*string{
			"#PARTITION_KEY": aws.String(SortKeyName),
			"#DATA":          aws.String(DataKeyName),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":OCCURRENCE": {
				S: aws.String(occurrenceSK),
			},
			":PROJECT": {
				S: aws.String(projectId),
			},
		},
		KeyConditionExpression: aws.String("#PARTITION_KEY=:OCCURRENCE AND #DATA=:PROJECT"),
		Limit:                  aws.Int64(int64(pageSize)),
	}

	if pageToken != "" {
		tokenArray := strings.Split(pageToken, PaginationString)
		if len(tokenArray) != 3 {
			log.Print("Error when trying to parse page token")
			return occurrences, "", nil
		}

		queryInput.ExclusiveStartKey = map[string]*dynamodb.AttributeValue{
			PartitionKeyName: {
				S: aws.String(tokenArray[0]),
			},
			SortKeyName: {
				S: aws.String(tokenArray[1]),
			},
			DataKeyName: {
				S: aws.String(tokenArray[2]),
			},
		}
	}

	result, err := db.Query(&queryInput)

	if err != nil {
		log.Printf("Error when listing occurrences:\n%s", err)
		return occurrences, "", nil
	}

	for _, item := range result.Items {
		dataItem := DataItem{}
		err = dynamodbattribute.UnmarshalMap(item, &dataItem)
		if err != nil {
			log.Panicf("Failed to unmarshal item, %v", err)
		}

		var occurrence pb.Occurrence
		err = jsonpb.Unmarshal(strings.NewReader(dataItem.Json), &occurrence)
		if err != nil {
			log.Panicf("Failed to unmarshal json, %v", err)
		}
		occurrences = append(occurrences, &occurrence)
	}

	var token = ""
	if result.LastEvaluatedKey != nil {
		pk := *result.LastEvaluatedKey[PartitionKeyName].S
		sk := *result.LastEvaluatedKey[SortKeyName].S
		dk := *result.LastEvaluatedKey[DataKeyName].S
		if pk == "" || sk == "" || dk == "" {
			log.Panicf("Unable to unmarshal LastEvaluatedKey")
		} else {
			token = fmt.Sprintf("%s%s%s%s%s", pk, PaginationString, sk, PaginationString, dk)
		}
	}

	return occurrences, token, nil
}

// CreateOccurrence creates the specified occurrence in storage.
func (db *DynamoDb) CreateOccurrence(ctx context.Context, projectId, userID string, o *pb.Occurrence) (*pb.Occurrence, error) {
	o = proto.Clone(o).(*pb.Occurrence)
	o.CreateTime = ptypes.TimestampNow()

	var oID string
	if nr, err := uuid.NewRandom(); err != nil {
		return nil, status.Error(codes.Internal, "Failed to generate UUID")
	} else {
		oID = nr.String()
	}
	oName := name.FormatOccurrence(projectId, oID)
	o.Name = oName

	m := jsonpb.Marshaler{}
	jsonObject, err := m.MarshalToString(o)
	if err != nil {
		log.Println("Unable to marshal occurrence into json", err)
		return nil, status.Error(codes.Internal, "Unable to marshal occurrence into json")
	}

	// use Global Primary Index for find by ID
	// use GSI for find all by type (OCCURRENCE), within project (Data)
	dataItem := DataItem{
		PartitionKey: oName,
		SortKey:      occurrenceSK,
		Data:         projectId,
		Json:         jsonObject,
	}

	av, err := dynamodbattribute.MarshalMap(dataItem)
	if err != nil {
		log.Println("Failed to marshal occurrence into AttributeValues", err)
		return nil, status.Error(codes.Internal, "Failed to marshal occurrence into AttributeValues")
	}

	// for notes within occurrence:
	// GPI(pk, sk): occurrence ID, nID
	// GSI(sk, data): NoteName, oName
	// For GSI, we put oName in data to achieve sorting or occurrences
	noteDataItem := DataItem{
		PartitionKey: oName,
		SortKey:      o.NoteName,
		Data:         oName,
		Json:         jsonObject,
	}

	nav, err := dynamodbattribute.MarshalMap(noteDataItem)
	if err != nil {
		log.Println("Failed to marshal occurrence into AttributeValues", err)
		return nil, status.Error(codes.Internal, "Failed to marshal occurrence into AttributeValues")
	}

	input := &dynamodb.TransactWriteItemsInput{
		TransactItems: []*dynamodb.TransactWriteItem{
			{
				Put: &dynamodb.Put{
					Item:                av,
					TableName:           aws.String(db.TableName),
					ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s) AND attribute_not_exists(%s)", PartitionKeyName, SortKeyName)),
				},
			},
			{
				Put: &dynamodb.Put{
					Item:      nav,
					TableName: aws.String(db.TableName),
				},
			},
		},
	}
	_, err = db.TransactWriteItems(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "ConditionalCheckFailedException" {
			return nil, status.Errorf(codes.AlreadyExists, "Occurrence with name %q already exists", projectId)
		} else {
			log.Println("Failed to insert Occurrence in database", err)
			return nil, status.Error(codes.Internal, "Failed to insert Occurrence in database")
		}
	}

	return o, nil
}

// BatchCreateOccurrences batch creates the specified occurrences in storage.
func (db *DynamoDb) BatchCreateOccurrences(ctx context.Context, projectId string, userID string, occs []*pb.Occurrence) ([]*pb.Occurrence, []error) {
	clonedOccs := []*pb.Occurrence{}
	for _, o := range occs {
		clonedOccs = append(clonedOccs, proto.Clone(o).(*pb.Occurrence))
	}
	occs = clonedOccs

	errs := []error{}
	created := []*pb.Occurrence{}
	for _, o := range occs {
		occ, err := db.CreateOccurrence(ctx, projectId, userID, o)
		if err != nil {
			// Occurrence already exists, skipping.
			continue
		} else {
			created = append(created, occ)
		}
	}

	return created, errs
}

// UpdateOccurrence updates the specified occurrence in storage.
func (db *DynamoDb) UpdateOccurrence(ctx context.Context, projectId, occId string, o *pb.Occurrence, mask *fieldmaskpb.FieldMask) (*pb.Occurrence, error) {
	o = proto.Clone(o).(*pb.Occurrence)

	// TODO(#312): implement the update operation
	o.UpdateTime = ptypes.TimestampNow()

	m := jsonpb.Marshaler{}
	jsonObject, err := m.MarshalToString(o)

	if err != nil {
		log.Println("Unable to marshal occurrence into json", err)
		return nil, status.Error(codes.Internal, "Unable to marshal occurrence into json")
	}

	// use Global Primary Index for find by ID
	// use GSI for find all by type (OCCURRENCE), within project (Data)
	dataItem := DataItem{
		PartitionKey: o.Name,
		SortKey:      occurrenceSK,
		Data:         projectId,
		Json:         jsonObject,
	}

	av, err := dynamodbattribute.MarshalMap(dataItem)
	if err != nil {
		log.Println("Failed to marshal occurrence into AttributeValues", err)
		return nil, status.Error(codes.Internal, "Failed to marshal occurrence into AttributeValues")
	}

	// for notes within occurrence:
	// GPI(pk, sk): occurrence ID, nID
	// GSI(sk, data): NoteName, oName
	// For GSI, we put oName in data to achieve sorting or occurrences
	noteDataItem := DataItem{
		PartitionKey: o.Name,
		SortKey:      o.NoteName,
		Data:         o.Name,
		Json:         jsonObject,
	}

	nav, err := dynamodbattribute.MarshalMap(noteDataItem)
	if err != nil {
		log.Println("Failed to marshal occurrence into AttributeValues", err)
		return nil, status.Error(codes.Internal, "Failed to marshal occurrence into AttributeValues")
	}

	input := &dynamodb.TransactWriteItemsInput{
		TransactItems: []*dynamodb.TransactWriteItem{
			{
				Put: &dynamodb.Put{
					Item:                av,
					TableName:           aws.String(db.TableName),
					ConditionExpression: aws.String(fmt.Sprintf("attribute_exists(%s) AND attribute_exists(%s)", PartitionKeyName, SortKeyName)),
				},
			},
			{
				Put: &dynamodb.Put{
					Item:      nav,
					TableName: aws.String(db.TableName),
				},
			},
		},
	}
	_, err = db.TransactWriteItems(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "ConditionalCheckFailedException" {
			return nil, status.Errorf(codes.AlreadyExists, "Occurrence with name %q already exists", projectId)
		} else {
			log.Println("Failed to insert Occurrence in database", err)
			return nil, status.Error(codes.Internal, "Failed to insert Occurrence in database")
		}
	}

	return o, nil
}

// DeleteOccurrence deletes the specified occurrence in storage.
func (db *DynamoDb) DeleteOccurrence(ctx context.Context, projectId, occId string) error {
	oName := name.FormatOccurrence(projectId, occId)

	// we need to delete both the main occurrence and the auxiliary entry that maps occurrence -> note
	// we can't delete multiple items using a wildcard pk of the oID: we actually need to
	// get the [pk, sk] pair for both entries and delete these, which requires a get/delete
	o, err := db.GetOccurrence(ctx, projectId, occId)
	if err != nil {
		return err
	}

	input := &dynamodb.TransactWriteItemsInput{
		TransactItems: []*dynamodb.TransactWriteItem{
			{
				Delete: &dynamodb.Delete{
					TableName: aws.String(db.TableName),
					Key: map[string]*dynamodb.AttributeValue{
						PartitionKeyName: {
							S: aws.String(oName),
						},
						SortKeyName: {
							S: aws.String(occurrenceSK),
						},
					},
					ConditionExpression: aws.String(fmt.Sprintf("attribute_exists(%s) AND attribute_exists(%s)", PartitionKeyName, SortKeyName)),
				},
			},
			{
				Delete: &dynamodb.Delete{
					TableName: aws.String(db.TableName),
					Key: map[string]*dynamodb.AttributeValue{
						PartitionKeyName: {
							S: aws.String(oName),
						},
						SortKeyName: {
							S: aws.String(o.NoteName),
						},
					},
				},
			},
		},
	}
	_, err = db.TransactWriteItems(input)
	if err != nil {
		return err
	}

	return nil
}

// GetNote gets the specified note from storage.
func (db *DynamoDb) GetNote(ctx context.Context, projectId, nID string) (*pb.Note, error) {
	result, err := db.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(db.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			PartitionKeyName: {
				S: aws.String(name.FormatNote(projectId, nID)),
			},
			SortKeyName: {
				S: aws.String(noteSK),
			},
		},
		ConsistentRead: aws.Bool(true),
	})

	if err != nil {
		log.Printf("Error when seeking note %s/%s, %s", projectId, nID, err)
		return nil, nil
	}

	dataItem := DataItem{}
	err = dynamodbattribute.UnmarshalMap(result.Item, &dataItem)
	if err != nil {
		log.Panicf("Failed to unmarshal item, %v", err)
	}

	if dataItem.PartitionKey == "" {
		return nil, status.Errorf(codes.NotFound, "Note with name %s/%s does not exist", projectId, nID)
	}

	var note pb.Note
	err = jsonpb.Unmarshal(strings.NewReader(dataItem.Json), &note)
	if err != nil {
		log.Panicf("Failed to unmarshal json, %v\n%s", err, dataItem.Json)
	}

	return &note, nil
}

// ListNotes lists notes for the specified project from storage.
func (db *DynamoDb) ListNotes(ctx context.Context, projectId, filter, pageToken string, pageSize int32) ([]*pb.Note, string, error) {
	var notes []*pb.Note

	queryInput := dynamodb.QueryInput{
		TableName: aws.String(db.TableName),
		IndexName: aws.String(GlobalSecondaryIndex1),
		ExpressionAttributeNames: map[string]*string{
			"#PARTITION_KEY": aws.String(SortKeyName),
			"#DATA":          aws.String(DataKeyName),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":NOTE": {
				S: aws.String(noteSK),
			},
			":PROJECT": {
				S: aws.String(projectId),
			},
		},
		KeyConditionExpression: aws.String("#PARTITION_KEY=:NOTE AND #DATA=:PROJECT"),
		Limit:                  aws.Int64(int64(pageSize)),
	}

	if pageToken != "" {
		tokenArray := strings.Split(pageToken, PaginationString)
		if len(tokenArray) != 3 {
			log.Print("Error when trying to parse page token")
			return notes, "", nil
		}

		queryInput.ExclusiveStartKey = map[string]*dynamodb.AttributeValue{
			PartitionKeyName: {
				S: aws.String(tokenArray[0]),
			},
			SortKeyName: {
				S: aws.String(tokenArray[1]),
			},
			DataKeyName: {
				S: aws.String(tokenArray[2]),
			},
		}
	}

	result, err := db.Query(&queryInput)

	if err != nil {
		log.Printf("Error when listing notes:\n%s", err)
		return notes, "", nil
	}

	for _, item := range result.Items {
		dataItem := DataItem{}
		err = dynamodbattribute.UnmarshalMap(item, &dataItem)
		if err != nil {
			log.Panicf("Failed to unmarshal item, %v", err)
		}

		var note pb.Note
		err = jsonpb.Unmarshal(strings.NewReader(dataItem.Json), &note)
		if err != nil {
			log.Panicf("Failed to unmarshal json, %v", err)
		}
		notes = append(notes, &note)
	}

	var token = ""
	if result.LastEvaluatedKey != nil {
		pk := *result.LastEvaluatedKey[PartitionKeyName].S
		sk := *result.LastEvaluatedKey[SortKeyName].S
		dk := *result.LastEvaluatedKey[DataKeyName].S
		if pk == "" || sk == "" || dk == "" {
			log.Panicf("Unable to unmarshal LastEvaluatedKey")
		} else {
			token = fmt.Sprintf("%s%s%s%s%s", pk, PaginationString, sk, PaginationString, dk)
		}
	}

	return notes, token, nil
}

// CreateNote creates the specified note in storage.
func (db *DynamoDb) CreateNote(ctx context.Context, projectId, nID string, userID string, n *pb.Note) (*pb.Note, error) {
	n = proto.Clone(n).(*pb.Note)
	nName := name.FormatNote(projectId, nID)
	n.Name = nName
	n.CreateTime = ptypes.TimestampNow()

	m := jsonpb.Marshaler{}
	jsonObject, err := m.MarshalToString(n)

	if err != nil {
		log.Println("Unable to marshal occurrence into json", err)
		return nil, status.Error(codes.Internal, "Unable to marshal occurrence into json")
	}

	// use Global Primary Index for find by ID, where ID is composite of project ID / note ID
	// use GSI for find all by type (NOTE), within project (Data)
	dataItem := DataItem{
		PartitionKey: nName,
		SortKey:      noteSK,
		Data:         projectId,
		Json:         jsonObject,
	}

	av, err := dynamodbattribute.MarshalMap(dataItem)
	if err != nil {
		log.Println("Failed to marshal note into AttributeValues", err)
		return nil, status.Error(codes.Internal, "Failed to marshal note into AttributeValues")
	}

	input := &dynamodb.PutItemInput{
		Item:                av,
		TableName:           aws.String(db.TableName),
		ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s) AND attribute_not_exists(%s)", PartitionKeyName, SortKeyName)),
	}

	_, err = db.PutItem(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "ConditionalCheckFailedException" {
			return nil, status.Errorf(codes.AlreadyExists, "Note with name %q already exists", n.Name)
		} else {
			log.Println("Failed to insert note in database", err)
			return nil, status.Error(codes.Internal, "Failed to insert Note in database")
		}
	}

	return n, nil
}

// BatchCreateNotes batch creates the specified notes in storage.
func (db *DynamoDb) BatchCreateNotes(ctx context.Context, projectId string, userID string, notes map[string]*pb.Note) ([]*pb.Note, []error) {
	clonedNotes := map[string]*pb.Note{}
	for nID, n := range notes {
		clonedNotes[nID] = proto.Clone(n).(*pb.Note)
	}
	notes = clonedNotes

	errs := []error{}
	created := []*pb.Note{}
	for nID, n := range notes {
		note, err := db.CreateNote(ctx, projectId, nID, userID, n)
		if err != nil {
			// Note already exists, skipping.
			continue
		} else {
			created = append(created, note)
		}

	}

	return created, errs
}

// UpdateNote updates the specified note in storage.
func (db *DynamoDb) UpdateNote(ctx context.Context, projectId, nID string, n *pb.Note, mask *fieldmaskpb.FieldMask) (*pb.Note, error) {
	n = proto.Clone(n).(*pb.Note)
	nName := name.FormatNote(projectId, nID)
	n.Name = nName

	// TODO(#312): implement the update operation
	n.UpdateTime = ptypes.TimestampNow()

	m := jsonpb.Marshaler{}
	jsonObject, err := m.MarshalToString(n)

	if err != nil {
		log.Println("Unable to marshal occurrence into json", err)
		return nil, status.Error(codes.Internal, "Unable to marshal occurrence into json")
	}

	// use Global Primary Index for find by ID, where ID is composite of project ID / note ID
	// use GSI for find all by type (NOTE), within project (Data)
	dataItem := DataItem{
		PartitionKey: nName,
		SortKey:      noteSK,
		Data:         projectId,
		Json:         jsonObject,
	}

	av, err := dynamodbattribute.MarshalMap(dataItem)
	if err != nil {
		log.Println("Failed to marshal note into AttributeValues", err)
		return nil, status.Error(codes.Internal, "Failed to marshal note into AttributeValues")
	}

	input := &dynamodb.PutItemInput{
		Item:                av,
		TableName:           aws.String(db.TableName),
		ConditionExpression: aws.String(fmt.Sprintf("attribute_exists(%s) AND attribute_exists(%s)", PartitionKeyName, SortKeyName)),
	}

	_, err = db.PutItem(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "ConditionalCheckFailedException" {
			return nil, status.Errorf(codes.AlreadyExists, "Note with name %q does not exist", n.Name)
		} else {
			log.Println("Failed to insert note in database", err)
			return nil, status.Error(codes.Internal, "Failed to insert Note in database")
		}
	}

	return n, nil
}

// DeleteNote deletes the specified note in storage.
func (db *DynamoDb) DeleteNote(ctx context.Context, projectId, nID string) error {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			PartitionKeyName: {
				S: aws.String(name.FormatNote(projectId, nID)),
			},
			SortKeyName: {
				S: aws.String(noteSK),
			},
		},
		TableName:           aws.String(db.TableName),
		ConditionExpression: aws.String(fmt.Sprintf("attribute_exists(%s) AND attribute_exists(%s)", PartitionKeyName, SortKeyName)),
	}

	_, err := db.DeleteItem(input)
	if err != nil {
		return err
	}

	return nil
}

// GetOccurrenceNote gets the note for the specified occurrence from storage.
func (db *DynamoDb) GetOccurrenceNote(ctx context.Context, projectId, oID string) (*pb.Note, error) {
	o, err := db.GetOccurrence(ctx, projectId, oID)
	if err != nil {
		return nil, err
	}
	nPID, nID, _ := name.ParseNote(o.NoteName)
	n, err := db.GetNote(ctx, nPID, nID)
	if err != nil {
		return nil, err
	}

	return n, nil
}

// ListNoteOccurrences lists all occurrences across all projects for the specified note from storage.
func (db *DynamoDb) ListNoteOccurrences(ctx context.Context, nPID, nID, filter, pageToken string, pageSize int32) ([]*pb.Occurrence, string, error) {
	var occurrences []*pb.Occurrence

	noteName := name.FormatNote(nPID, nID)

	queryInput := dynamodb.QueryInput{
		TableName: aws.String(db.TableName),
		IndexName: aws.String(GlobalSecondaryIndex1),
		ExpressionAttributeNames: map[string]*string{
			"#PARTITION_KEY": aws.String(SortKeyName),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":NOTE_NAME": {
				S: &noteName,
			},
		},
		KeyConditionExpression: aws.String("#PARTITION_KEY=:NOTE_NAME"),
		Limit:                  aws.Int64(int64(pageSize)),
	}

	if pageToken != "" {
		tokenArray := strings.Split(pageToken, PaginationString)
		if len(tokenArray) != 3 {
			log.Print("Error when trying to parse page token")
			return occurrences, "", nil
		}

		queryInput.ExclusiveStartKey = map[string]*dynamodb.AttributeValue{
			PartitionKeyName: {
				S: aws.String(tokenArray[0]),
			},
			SortKeyName: {
				S: aws.String(tokenArray[1]),
			},
			DataKeyName: {
				S: aws.String(tokenArray[2]),
			},
		}
	}

	result, err := db.Query(&queryInput)

	if err != nil {
		log.Printf("Error when listing occurrences:\n%s", err)
		return occurrences, "", nil
	}

	for _, item := range result.Items {
		dataItem := DataItem{}
		err = dynamodbattribute.UnmarshalMap(item, &dataItem)
		if err != nil {
			log.Panicf("Failed to unmarshal item, %v", err)
		}

		var occurrence pb.Occurrence
		err = jsonpb.Unmarshal(strings.NewReader(dataItem.Json), &occurrence)
		if err != nil {
			log.Panicf("Failed to unmarshal json, %v", err)
		}
		occurrences = append(occurrences, &occurrence)
	}

	var token = ""
	if result.LastEvaluatedKey != nil {
		pk := *result.LastEvaluatedKey[PartitionKeyName].S
		sk := *result.LastEvaluatedKey[SortKeyName].S
		dk := *result.LastEvaluatedKey[DataKeyName].S
		if pk == "" || sk == "" || dk == "" {
			log.Panicf("Unable to unmarshal LastEvaluatedKey")
		} else {
			token = fmt.Sprintf("%s%s%s%s%s", pk, PaginationString, sk, PaginationString, dk)
		}
	}

	return occurrences, token, nil
}

// GetVulnerabilityOccurrencesSummary gets a summary of vulnerability occurrences from storage.
func (db *DynamoDb) GetVulnerabilityOccurrencesSummary(ctx context.Context, projectId, filter string) (*pb.VulnerabilityOccurrencesSummary, error) {
	return &pb.VulnerabilityOccurrencesSummary{}, nil
}
