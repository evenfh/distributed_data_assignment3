package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/spacycoder/exercise3/pkg/activity"
	"github.com/spacycoder/exercise3/pkg/trackpoint"
	"github.com/spacycoder/exercise3/pkg/user"
)

func worker(tracker chan empty, users chan user.User, userService *user.Service, activityService *activity.Service, trackpointService *trackpoint.Service) {
	trackpoints := make([]trackpoint.Trackpoint, 2500, 2500)

	for u := range users {
		path := fmt.Sprintf("./dataset/Data/%s/Trajectory", u.ID)
		files, err := ioutil.ReadDir(path)
		if err != nil {
			panic(err)
		}

		for _, file := range files {
			err := createTrajectories(u, file.Name(), userService, activityService, trackpointService, trackpoints)
			if err != nil {
				panic(err)
			}
		}
	}

	var e empty
	tracker <- e
}

func loadDataset(config *Config, userService *user.Service, activityService *activity.Service, trackpointService *trackpoint.Service) error {
	fmt.Println("Loading dataset")

	if err := insertUsers(userService); err != nil {
		return err
	}

	usersChan := make(chan user.User, config.WorkerCount)
	tracker := make(chan empty)

	users, err := userService.GetUsers()
	if err != nil {
		return err
	}

	startTime := time.Now()
	// start workers
	for i := 0; i < config.WorkerCount; i++ {
		go worker(tracker, usersChan, userService, activityService, trackpointService)
	}

	// push users to workers
	for _, u := range users {
		usersChan <- u
	}
	close(usersChan)

	// wait for workers to finish
	for i := 0; i < config.WorkerCount; i++ {
		<-tracker
	}

	fmt.Printf("Finished loading %s\n", time.Since(startTime))
	fmt.Println("Creating indexes...")
	if err := activityService.CreateIndexes(); err != nil {
		return err
	}

	if err := trackpointService.CreateIndexes(); err != nil {
		return err
	}
	fmt.Println("Finished creating indexes")
	return nil
}

func insertUsers(userService *user.Service) error {
	f, err := os.Open("./dataset/labeled_ids.txt")
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(f)
	labeledUsers := make(map[string]struct{})

	for scanner.Scan() {
		user := scanner.Text()
		if strings.TrimSpace(user) == "" {
			continue
		}

		labeledUsers[user] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	files, err := ioutil.ReadDir("./dataset/Data/")
	if err != nil {
		return err
	}
	for _, file := range files {
		_, exists := labeledUsers[file.Name()]
		err := userService.CreateUser(file.Name(), exists)
		if err != nil {
			return err
		}
	}

	return nil
}

func createTrajectories(user user.User, fileName string, userService *user.Service, activityService *activity.Service, trackpointService *trackpoint.Service, trackpoints []trackpoint.Trackpoint) error {
	filePath := fmt.Sprintf("./dataset/Data/%s/Trajectory/%s", user.ID, fileName)
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	valid, err := isValidLineCount(f)
	if err != nil {
		return err
	}
	if !valid {
		return nil
	}
	f.Seek(0, io.SeekStart)
	scanner := bufio.NewScanner(f)
	// skip first 6 lines
	scanner.Scan()
	scanner.Scan()
	scanner.Scan()
	scanner.Scan()
	scanner.Scan()
	scanner.Scan()

	var activities []activity.Activity
	if user.HasLabels {
		activities, err = getActivities(user, activityService)
		if err != nil {
			return err
		}
	}

	trackpointIndex := 0
	for scanner.Scan() {
		row := scanner.Text()
		row = strings.TrimSpace(row)
		if row == "" {
			continue
		}

		cols := strings.Split(row, ",")
		if len(cols) != 7 {
			continue
		}
		layout := "2006-01-02T15:04:05"

		date, err := time.Parse(layout, cols[5]+"T"+cols[6])
		if err != nil {
			return err
		}

		lat, err := strconv.ParseFloat(cols[0], 64)
		if err != nil {
			return err
		}
		lon, err := strconv.ParseFloat(cols[1], 64)
		if err != nil {
			return err
		}
		alt, err := strconv.ParseFloat(cols[3], 64) // alt is in some cases float
		if err != nil {
			return err
		}
		days, err := strconv.ParseFloat(cols[4], 64)
		if err != nil {
			return err
		}

		trackpoints[trackpointIndex].Altitude = int(alt)
		trackpoints[trackpointIndex].Location = trackpoint.Location{
			Coordiantes: []float64{lon, lat},
			Type:        "Point",
		}
		trackpoints[trackpointIndex].DateTime = date
		trackpoints[trackpointIndex].DateDays = days
		trackpoints[trackpointIndex].UserID = user.ID
		trackpointIndex++
	}

	if trackpointIndex > 0 {
		var transportMode *string
		if len(activities) > 0 {
			startDate := trackpoints[0].DateTime
			endDate := trackpoints[trackpointIndex-1].DateTime

			for _, act := range activities {
				if act.StartDateTime.Equal(startDate) && act.EndDateTime.Equal(endDate) {
					transportMode = act.TransportationMode
					break
				}
			}
		}

		act := activity.Activity{
			UserID:             &user.ID,
			StartDateTime:      trackpoints[0].DateTime,
			EndDateTime:        trackpoints[trackpointIndex-1].DateTime,
			TransportationMode: transportMode,
		}

		activityID, err := activityService.CreateActivity(act)
		if err != nil {
			return err
		}

		err = userService.AddActivity(user.ID, activityID)
		if err != nil {
			panic(err)
		}
		for i := 0; i < trackpointIndex; i++ {
			trackpoints[i].ActivityID = activityID
		}
	} else {
		fmt.Println("Trackpoint index == 00")
		fmt.Println(user)
	}

	if trackpointIndex > 0 {
		return trackpointService.BulkInsertTrackpoint(trackpoints, trackpointIndex)
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func getActivities(user user.User, activityService *activity.Service) ([]activity.Activity, error) {
	activityPath := fmt.Sprintf("./dataset/Data/%s/labels.txt", user.ID)
	f, err := os.Open(activityPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	activities := []activity.Activity{}
	// skip first line
	scanner.Scan()
	for scanner.Scan() {
		row := scanner.Text()
		row = strings.TrimSpace(row)
		if row == "" {
			continue
		}
		cols := strings.Split(row, "\t")

		startDate, err := time.Parse(dateLayout, cols[0])
		if err != nil {
			return nil, err
		}
		endDate, err := time.Parse(dateLayout, cols[1])
		if err != nil {
			return nil, err
		}

		act := activity.Activity{
			UserID:             &user.ID,
			TransportationMode: &cols[2],
			StartDateTime:      startDate,
			EndDateTime:        endDate,
		}
		activities = append(activities, act)
	}

	return activities, nil
}

func isValidLineCount(r io.Reader) (bool, error) {
	buf := make([]byte, 1024*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)
		if count >= validLineCount {
			return false, nil
		}
		switch {
		case err == io.EOF:
			return true, nil

		case err != nil:
			return false, err
		}
	}
}

func getActivityIDForTrackpoint(timestamp time.Time, activities []activity.Activity) (int, *primitive.ObjectID) {
	indexOffset := 0
	if activities[indexOffset].StartDateTime.After(timestamp) {
		return 0, nil
	}
	if activities[indexOffset].EndDateTime.After(timestamp) {
		return 0, activities[indexOffset].ID
	}

	indexOffset++
	if indexOffset >= len(activities) {
		return indexOffset, nil
	}
	if activities[indexOffset].StartDateTime.After(timestamp) {
		return indexOffset, nil
	}

	return indexOffset, activities[indexOffset].ID
}
