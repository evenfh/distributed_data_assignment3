package activity

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func NewService(db *mongo.Database) (*Service, error) {
	return &Service{db: db}, nil
}

type Service struct {
	db *mongo.Database
}

func (a *Service) CreateIndexes() error {
	coll := a.db.Collection("activities")
	transportationModeIndex := mongo.IndexModel{
		Keys: bson.M{
			"transportation_mode": 1, // index in ascending order
		}, Options: nil,
	}

	_, err := coll.Indexes().CreateOne(context.TODO(), transportationModeIndex)
	return err
}

func (a *Service) CreateActivity(act Activity) (primitive.ObjectID, error) {
	collection := a.db.Collection("activities")
	res, err := collection.InsertOne(context.TODO(), act)
	if err != nil {
		return primitive.ObjectID{}, err
	}

	id, ok := res.InsertedID.(primitive.ObjectID)
	if !ok {
		fmt.Println("err", res)
		panic("DAWDWAD")
	}
	return id, nil
}

func (a *Service) AverageActivitesPerUser() (float64, error) {
	collection := a.db.Collection("users")

	countStage := bson.D{{
		"$project", bson.M{
			"_id": nil, "count": bson.M{"$size": "$activities"},
		}}}

	avgStage := bson.D{{
		"$group", bson.M{
			"_id": nil, "avgCount": bson.M{"$avg": "$count"},
		}}}
	cursor, err := collection.Aggregate(context.Background(), mongo.Pipeline{countStage, avgStage})
	if err != nil {
		return 0, err
	}
	defer cursor.Close(context.Background())

	avg := 0.0
	hasValue := cursor.TryNext(context.Background())
	if !hasValue {
		return 0.0, errors.New("No values returned")
	}
	res := cursor.Current
	avgCount := res.Lookup("avgCount")
	avg = avgCount.Double()
	return avg, err
}

func (a *Service) YearWithMostActivites() (int, int, error) {
	collection := a.db.Collection("activities")
	countStage := bson.D{{
		"$group", bson.M{
			"_id": bson.M{
				"$year": "$start_date_time",
			},
			"count": bson.M{"$sum": 1},
		},
	},
	}
	sortStage := bson.D{{"$sort", bson.M{"count": -1}}}
	limitStage := bson.D{{"$limit", 1}}
	cursor, err := collection.Aggregate(context.Background(), mongo.Pipeline{countStage, sortStage, limitStage})
	if err != nil {
		return 0, 0, err
	}
	defer cursor.Close(context.Background())

	if !cursor.TryNext(context.Background()) {
		return 0, 0, errors.New("Empty return value")
	}

	curr := cursor.Current
	year := curr.Lookup("_id").Int32()
	count := curr.Lookup("count").Int32()
	return int(year), int(count), nil
}

func (a *Service) YearWithMostHours() (int, int, error) {
	// query := "SELECT YEAR(start_date_time) as year, SUM(TIMESTAMPDIFF(hour,start_date_time, end_date_time)) duration FROM Activity GROUP BY YEAR(start_date_time) ORDER BY duration DESC LIMIT 1"

	collection := a.db.Collection("activities")
	projectStage := bson.D{
		{"$project",
			bson.M{
				"start_date_time": "$start_date_time",
				"diff": bson.M{
					"$divide": bson.A{
						bson.M{"$subtract": bson.A{"$end_date_time", "$start_date_time"}},
						60 * 1000 * 60, // convert to hours
					},
				},
			},
		}}
	countStage := bson.D{{"$group", bson.M{"_id": bson.M{"$year": "$start_date_time"}, "sum": bson.M{"$sum": "$diff"}}}}
	sortStage := bson.D{{"$sort", bson.M{"sum": -1}}}
	limitStage := bson.D{{"$limit", 1}}

	cursor, err := collection.Aggregate(context.Background(), mongo.Pipeline{projectStage, countStage, sortStage, limitStage})
	if err != nil {
		return 0, 0, err
	}
	defer cursor.Close(context.Background())

	if !cursor.TryNext(context.Background()) {
		return 0, 0, errors.New("Empty return value")
	}

	curr := cursor.Current
	year := curr.Lookup("_id").Int32()
	count := curr.Lookup("sum").Double()
	return int(year), int(count), nil
}

func (a *Service) GetActivityIDForUserWithTimestamp(userID string, timeStamp time.Time) (*int, error) {
	panic("NOT IMPLEMENTED")
}

func (a *Service) GetCount() (int, error) {
	count, err := a.db.Collection("activities").CountDocuments(context.TODO(), bson.M{})
	return int(count), err
}

func (a *Service) GetUsersActivityCount(limit int) ([]string, []int, error) {
	collection := a.db.Collection("users")
	// matchStage := bson.D{{"$match", bson.M{"activities": bson.M{"$not": bson.M{"$size": 0}}}}}
	countStage := bson.D{{"$project", bson.M{"_id": "$_id", "count": bson.M{"$size": "$activities"}}}}
	sortStage := bson.D{{"$sort", bson.M{"count": -1}}}
	limitStage := bson.D{{"$limit", limit}}
	cursor, err := collection.Aggregate(context.Background(), mongo.Pipeline{countStage, sortStage, limitStage})
	if err != nil {
		return nil, nil, err
	}
	defer cursor.Close(context.Background())

	ids := []string{}
	counts := []int{}
	for cursor.TryNext(context.Background()) {
		doc := cursor.Current
		id := doc.Lookup("_id")
		count := doc.Lookup("count").Int32()
		ids = append(ids, id.String())
		counts = append(counts, int(count))
	}
	return ids, counts, nil
}

type UserTransportationCount struct {
	Mode  string `bson:"transportation_mode"`
	Count int32  `bson:"count"`
}

func (a *Service) GetTopTransportationByUsers() ([]string, []UserTransportationCount, error) {
	collection := a.db.Collection("activities")
	matchStage := bson.D{{"$match", bson.M{"transportation_mode": bson.M{"$exists": true}}}}
	countStage := bson.D{{"$group", bson.M{"_id": bson.M{"transportation_mode": "$transportation_mode", "user": "$user_id"}, "count": bson.M{"$sum": 1}}}}
	sortStage := bson.D{{"$sort", bson.M{"count": -1}}}
	cursor, err := collection.Aggregate(context.Background(), mongo.Pipeline{matchStage, countStage, sortStage})
	if err != nil {
		return nil, nil, err
	}
	userTransCountMap := make(map[string]UserTransportationCount)
	defer cursor.Close(context.Background())
	for cursor.TryNext(context.Background()) {
		doc := cursor.Current

		uID := doc.Lookup("_id").Document().Lookup("user").String()
		mode := doc.Lookup("_id").Document().Lookup("transportation_mode").String()
		count := doc.Lookup("count").Int32()

		if _, ok := userTransCountMap[uID]; ok {
			continue
		}
		userTransCountMap[uID] = UserTransportationCount{Mode: mode, Count: count}
	}

	users := []string{}
	for u, _ := range userTransCountMap {
		users = append(users, u)
	}

	counts := make([]UserTransportationCount, len(users))
	sort.Strings(users)
	for i, u := range users {
		count := userTransCountMap[u]
		counts[i] = count
	}

	return users, counts, nil
}

func (a *Service) GetTransportationCounts() ([]string, []int, error) {
	collection := a.db.Collection("activities")
	matchStage := bson.D{{"$match", bson.M{"transportation_mode": bson.M{"$exists": true, "$ne": nil}}}}
	countStage := bson.D{{"$group", bson.M{"_id": "$transportation_mode", "count": bson.M{"$sum": 1}}}}
	sortStage := bson.D{{
		"$sort", bson.M{
			"count": -1,
		},
	}}
	cursor, err := collection.Aggregate(context.Background(), mongo.Pipeline{matchStage, countStage, sortStage})
	if err != nil {
		return nil, nil, err
	}
	defer cursor.Close(context.Background())

	transportationModes := []string{}
	counts := []int{}
	for cursor.TryNext(context.Background()) {
		doc := cursor.Current
		transportationModes = append(transportationModes, doc.Lookup("_id").String())
		counts = append(counts, int(doc.Lookup("count").Int32()))
	}

	return transportationModes, counts, err
}

func (a *Service) GetDistanceWalkedByUser(userId string) (float64, error) {
	collection := a.db.Collection("activities")

	projectStage := bson.D{{
		"$project", bson.M{
			"transportation_mode": "$transportation_mode",
			"yearStart": bson.M{
				"$year": "$start_date_time",
			},
			"yearEnd": bson.M{
				"$year": "$end_date_time",
			},
			"user_id": "$user_id",
		},
	}}

	matchStage := bson.D{{
		"$match", bson.M{
			"transportation_mode": "walk",
			"user_id":             userId,
			"yearStart":           2008,
			"yearEnd":             2008,
		},
	}}

	lookupStage := bson.D{{
		"$lookup", bson.M{
			"from":         "trackpoints",
			"localField":   "_id",
			"foreignField": "activity_id",
			"as":           "trackpoints",
		},
	}}

	cursor, err := collection.Aggregate(context.TODO(), mongo.Pipeline{projectStage, matchStage, lookupStage})

	if err != nil {
		return 0.0, err
	}
	defer cursor.Close(context.Background())

	distance := 0.0
	for cursor.TryNext(context.TODO()) {
		doc := cursor.Current
		trackpoints, err := doc.Lookup("trackpoints").Array().Values()
		if err != nil {
			return 0.0, err
		}

		for i := 0; i < len(trackpoints)-1; i++ {
			prevTrackpoint := trackpoints[i].Document()
			currTrackpoint := trackpoints[i+1].Document()

			coordinates1, err := prevTrackpoint.Lookup("location").Document().Lookup("coordinates").Array().Values()
			if err != nil {
				return 0.0, err
			}
			coordinates2, err := currTrackpoint.Lookup("location").Document().Lookup("coordinates").Array().Values()
			if err != nil {
				return 0.0, err
			}
			lat1 := coordinates1[1].Double()
			lon1 := coordinates1[0].Double()
			lat2 := coordinates2[1].Double()
			lon2 := coordinates2[0].Double()

			distance += calculateDistance(lat1, lon1, lat2, lon2)
		}
	}

	return distance, nil
}

func calculateDistance(fromLat float64, fromLon float64, toLat float64, toLon float64) float64 {
	lat1 := fromLat * math.Pi / 180.0
	lon1 := fromLon * math.Pi / 180.0
	lat2 := toLat * math.Pi / 180.0
	lon2 := toLon * math.Pi / 180.0

	diffLat := lat2 - lat1
	diffLon := lon2 - lon1

	ans := math.Pow(math.Sin(diffLat/2.0), 2) + (math.Cos(lat1) * math.Cos(lat2) * math.Pow(math.Sin(diffLon/2.0), 2))
	ans = 2.0 * math.Asin(math.Sqrt(ans))

	earthRadius := 6371.0

	return ans * earthRadius
}
