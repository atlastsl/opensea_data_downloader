package helpers

import (
	"context"
	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"os"
)

func NewDatabaseConnection() (database *mongo.Database, err error) {
	_options := options.Client().ApplyURI(os.Getenv("DATABASE_URL"))
	//client, err := mgm.NewClient(_options)
	client, err := mongo.Connect(context.Background(), _options)
	if err != nil {
		return nil, err
	}
	err = client.Ping(context.Background(), readpref.Primary())
	if err != nil {
		return nil, err
	}
	database = client.Database(os.Getenv("DATABASE_NAME"))
	return database, nil
}

func CollectionInstance(database *mongo.Database, m mgm.Model) *mgm.Collection {
	collection := mgm.NewCollection(database, mgm.CollName(m))
	return collection
}

func CloseDatabaseConnection(database *mongo.Database) {
	_ = database.Client().Disconnect(context.Background())
}
