package trackpoint

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Location struct {
	Coordiantes []float64 `bson:"coordinates"`
	Type        string    `bson:"type"`
}

type Trackpoint struct {
	ID         *primitive.ObjectID `bson:"_id,omitempty"`
	ActivityID primitive.ObjectID  `bson:"activity_id,omitempty"`
	Location   Location            `bson:"location"`
	Altitude   int                 `bson:"altitude,omitempty"`
	DateDays   float64             `bson:"date_days,omitempty"`
	DateTime   time.Time           `bson:"date_time,omitempty"`
	UserID     string              `bson:"user_id,omitempty"`
}
