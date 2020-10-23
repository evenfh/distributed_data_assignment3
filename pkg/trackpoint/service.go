package trackpoint

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func NewService(db *mongo.Database) (*Service, error) {
	return &Service{db: db}, nil
}

type Service struct {
	db *mongo.Database
}

func (a *Service) CreateIndexes() error {
	coll := a.db.Collection("trackpoints")
	activityIDIndex := mongo.IndexModel{
		Keys: bson.M{
			"activity_id": 1, // index in ascending order
		}, Options: nil,
	}
	geo := mongo.IndexModel{
		Keys: bson.M{
			"location": "2dsphere", // geo index
		}, Options: nil,
	}

	_, err := coll.Indexes().CreateMany(context.TODO(), []mongo.IndexModel{activityIDIndex, geo})
	return err
}

func (t *Service) GetCount() (int, error) {
	count, err := t.db.Collection("trackpoints").CountDocuments(context.TODO(), bson.M{})
	return int(count), err
}

func (t *Service) BulkInsertTrackpoint(trackpoints []Trackpoint, numTrackpoints int) error {
	collection := t.db.Collection("trackpoints")

	interfaceSlice := make([]interface{}, numTrackpoints)
	for i := 0; i < numTrackpoints; i++ {
		interfaceSlice[i] = trackpoints[i]
	}
	_, err := collection.InsertMany(context.TODO(), interfaceSlice)
	if err != nil {
		return err
	}
	return nil
}

func (t *Service) Close() {

}
