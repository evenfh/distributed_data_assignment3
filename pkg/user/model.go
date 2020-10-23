package user

import "go.mongodb.org/mongo-driver/bson/primitive"

type User struct {
	ID         string               `bson:"_id"`
	HasLabels  bool                 `json:"hasLabels"`
	Activities []primitive.ObjectID `bson:"activities"`
}
