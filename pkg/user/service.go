package user

import (
	"context"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/spacycoder/exercise3/pkg/activity"
	"github.com/spacycoder/exercise3/pkg/trackpoint"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Service struct {
	db *mongo.Database
}

func NewService(db *mongo.Database) *Service {
	return &Service{
		db: db,
	}
}

func (s *Service) CreateUser(name string, hasLabels bool) error {
	user := User{
		ID:         name,
		HasLabels:  hasLabels,
		Activities: []primitive.ObjectID{},
	}
	collection := s.db.Collection("users")
	_, err := collection.InsertOne(context.TODO(), user)
	if err != nil {
		return err
	}
	return nil
}

func (s *Service) GetUsers() ([]User, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	collection := s.db.Collection("users")
	cur, err := collection.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	users := []User{}
	err = cur.All(ctx, &users)
	if err != nil {
		return nil, err
	}

	return users, nil
}

func (s *Service) AddActivity(userID string, activityID primitive.ObjectID) error {
	collection := s.db.Collection("users")
	filter := bson.M{"_id": userID}

	update := bson.D{
		{"$push", bson.M{"activities": activityID}},
	}
	_, err := collection.UpdateOne(context.TODO(), filter, update)
	return err
}

func (u *Service) GetUsersThatHasUsedTransportationMode(transportationMode string) ([]string, error) {
	activitiesCollection := u.db.Collection("activities")
	/* filter := bson.M{"transportation_mode": transportationMode}

	res, err := activitiesCollection.Find(context.TODO(), filter)
	if err != nil {
		return nil, err
	}
	defer res.Close(context.TODO())
	for res.TryNext(context.TODO()) {
		curr := res.Current
		fmt.Printf("User: %+v", curr)
	}
	*/
	transportationMode = strings.ToLower(transportationMode)
	filter := bson.M{"transportation_mode": transportationMode}
	res, err := activitiesCollection.Distinct(context.TODO(), "user_id", filter)
	if err != nil {
		return nil, err
	}

	users := make([]string, len(res))
	for i := 0; i < len(res); i++ {
		users[i] = res[i].(string)
	}
	return users, nil
}

type UserWithAltitude struct {
	UserID         string
	GainedAltitude float64
}

func (u *Service) GetCount() (int, error) {
	count, err := u.db.Collection("users").CountDocuments(context.TODO(), bson.M{})
	return int(count), err
}

func (u *Service) GetUsersWithActivities() ([]User, error) {
	panic("NOT IMPLEMENTED")
}

func (u *Service) GetUsersWithMostAltitude(numUsers int) ([]UserWithAltitude, error) {
	collection := u.db.Collection("activities")
	findOptions := options.Find()
	findOptions.SetSort(bson.M{"user_id": -1})
	filter := bson.M{"transportation_mode": bson.M{"$exists": true, "$eq": "walk"}}
	cur, err := collection.Find(context.TODO(), filter, findOptions)

	if err != nil {
		return nil, err
	}

	usersWithAltitude := make([]UserWithAltitude, 0)
	//matchStage := bson.D{{"$skip", bson.D{{"altitude", -777}}}}
	// conditionStage := bson.D{{"$cond"}}
	//"}}}}}}}}
	currUID := ""
	gainedAltitude := 0.0
	for cur.Next(context.TODO()) {
		var currActivity activity.Activity
		err := cur.Decode(&currActivity)
		if err != nil {
			return nil, err
		}

		if currUID != *currActivity.UserID {
			if currUID != "" {
				usersWithAltitude = append(usersWithAltitude, UserWithAltitude{UserID: currUID, GainedAltitude: gainedAltitude * 0.3048})
			}
			gainedAltitude = 0
			currUID = *currActivity.UserID
		}

		trackpointCollection := u.db.Collection("trackpoints")
		trackpointFilter := bson.M{"altitude": bson.M{"$ne": -777}, "activity_id": bson.M{"$eq": currActivity.ID}}
		trackpointCur, err := trackpointCollection.Find(context.TODO(), trackpointFilter)

		if err != nil {
			return nil, err
		}

		prevAltitude := -999
		for trackpointCur.Next(context.TODO()) {
			var currTrackpoint trackpoint.Trackpoint
			err = trackpointCur.Decode(&currTrackpoint)
			if err != nil {
				return nil, err
			}
			altitude := currTrackpoint.Altitude

			if prevAltitude == -999 {
				prevAltitude = altitude
				continue
			}
			if altitude > prevAltitude {
				gainedAltitude += float64(altitude - prevAltitude)
			}
			prevAltitude = altitude
		}
	}
	usersWithAltitude = append(usersWithAltitude, UserWithAltitude{UserID: currUID, GainedAltitude: gainedAltitude * 0.3048})

	sort.Slice(usersWithAltitude, func(i, j int) bool {
		return usersWithAltitude[i].GainedAltitude > usersWithAltitude[j].GainedAltitude
	})
	if len(usersWithAltitude) > numUsers {
		usersWithAltitude = usersWithAltitude[0:numUsers]
	}
	return usersWithAltitude, nil
}

func (u *Service) UsersInBeijing() ([]string, error) {
	collection := u.db.Collection("trackpoints")
	filter := bson.M{
		"location": bson.M{
			"$near": bson.M{
				"$geometry":    bson.M{"type": "Point", "coordinates": []float64{116.397, 39.916}},
				"$maxDistance": 100,
			},
		},
	}
	usersInBeijing := []string{}
	res, err := collection.Distinct(context.TODO(), "user_id", filter)

	if err != nil {
		return nil, err
	}

	for _, t := range res {
		usersInBeijing = append(usersInBeijing, t.(string))
	}
	return usersInBeijing, nil
}

func (u *Service) GetUsersWithInvalidActivites() ([]string, []int, error) {
	collection := u.db.Collection("activities")

	opts := options.Aggregate()
	opts.SetAllowDiskUse(true)

	lookupStage := bson.D{{
		"$lookup", bson.M{
			"from":         "trackpoints",
			"localField":   "_id",
			"foreignField": "activity_id",
			"as":           "trackpoints",
		},
	}}

	cursor, err := collection.Aggregate(context.TODO(), mongo.Pipeline{lookupStage}, opts)
	if err != nil {
		return nil, nil, err
	}
	defer cursor.Close(context.Background())

	invalidMap := make(map[string]int)
	for cursor.TryNext(context.Background()) {
		current := cursor.Current
		userId := current.Lookup("user_id").String()
		trackpoints, err := current.Lookup("trackpoints").Array().Values()

		if err != nil {
			return nil, nil, err
		}

		for i := 0; i < len(trackpoints)-1; i++ {
			prevDate := trackpoints[i].Document().Lookup("date_time").Time()
			currDate := trackpoints[i+1].Document().Lookup("date_time").Time()
			diff := math.Abs(currDate.Sub(prevDate).Minutes())
			if diff >= 5 {
				if value, ok := invalidMap[userId]; ok {
					invalidMap[userId] = value + 1
				} else {
					invalidMap[userId] = 1
				}
				break
			}
		}
	}

	users := []string{}

	for key, _ := range invalidMap {
		users = append(users, key)
	}

	counts := make([]int, len(users))
	sort.Strings(users)

	for i, u := range users {
		counts[i] = invalidMap[u]
	}

	return users, counts, nil
}
