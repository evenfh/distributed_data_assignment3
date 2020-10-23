package activity

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type ActivityMode string

const (
	BUS  ActivityMode = "bus"
	BIKE ActivityMode = "bike"
)

type Activity struct {
	ID                 *primitive.ObjectID  `bson:"_id,omitempty"`
	UserID             *string              `bson:"user_id,omitempty"`
	TransportationMode *string              `bson:"transportation_mode,omitempty"`
	StartDateTime      time.Time            `bson:"start_date_time,omitempty"`
	EndDateTime        time.Time            `bson:"end_date_time,omitempty"`
	Trackpoints        []primitive.ObjectID `bson:"trackpoints,omitempty"`
}

type SortByDate []Activity

func (a SortByDate) Len() int      { return len(a) }
func (a SortByDate) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SortByDate) Less(i, j int) bool {
	return a[i].StartDateTime.Before(a[j].StartDateTime)
}
