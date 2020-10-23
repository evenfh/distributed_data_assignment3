package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/spacycoder/exercise3/pkg/activity"
	"github.com/spacycoder/exercise3/pkg/trackpoint"
	"github.com/spacycoder/exercise3/pkg/user"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const dateLayout string = "2006/01/02 15:04:05"
const validLineCount int = 2506

type empty struct{}

type Config struct {
	WorkerCount int
	User        string
	Password    string
	DbURL       string
	Operation   string
}

func newDB(host, user, password, dbname string) (*mongo.Client, error) {
	credentials := options.Credential{
		Username: "myuser",
		Password: "strava",
	}
	// mongodb://myuser:strava@localhost:27017
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017").SetAuth(credentials))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func main() {
	cpus := runtime.NumCPU()
	fmt.Printf("Number of CPUs: %d\n", cpus)

	operation := flag.String("op", "exercises", "load,exercises,drop")
	flag.Parse()

	fmt.Println(*operation)
	cfg := Config{
		WorkerCount: cpus * 2,
		User:        "root",
		Password:    "root",
		DbURL:       "127.0.0.1",
		//DbURL:     "tdt4225-29.idi.ntnu.no",
		Operation: *operation,
	}
	if err := run(&cfg); err != nil {
		log.Fatalf("Exited with error: %v\n", err)
	}
}

func run(config *Config) error {
	client, err := newDB(config.DbURL, config.User, config.Password, "strava")
	if err != nil {
		return err
	}
	defer func() {
		if err = client.Disconnect(context.Background()); err != nil {
			panic(err)
		}
	}()

	err = client.Ping(context.Background(), readpref.Primary())
	if err != nil {
		return err
	}

	fmt.Println("Successfully connected to database")

	_, err = os.Stat("./dataset")
	if os.IsNotExist(err) {
		return errors.New("./dataset folder not found")
	}
	if err != nil {
		return err
	}

	db := client.Database("strava")

	userService := user.NewService(db)
	if err != nil {
		return err
	}

	if err != nil {
		panic(err)
	}

	activityService, err := activity.NewService(db)
	if err != nil {
		return err
	}

	trackpointService, err := trackpoint.NewService(db)
	if err != nil {
		return err
	}

	switch config.Operation {
	case "load":
		err := loadDataset(config, userService, activityService, trackpointService)
		if err != nil {
			return err
		}
	case "exercises":
		err := runExercises(activityService, trackpointService, userService)
		if err != nil {
			return err
		}
	case "drop":
		err = db.Drop(context.Background())
		if err != nil {
			return err
		}
		err = db.Drop(context.Background())
		if err != nil {
			return err
		}
		err = db.Drop(context.Background())
		if err != nil {
			return err
		}
		return nil
	default:
		return errors.New("Invalid operation: " + config.Operation)
	}

	return nil
}

func runExercises(activityService *activity.Service, trackpointService *trackpoint.Service, userService *user.Service) error {

	fmt.Println("------------------")
	fmt.Println("      Task 1      ")
	fmt.Println("------------------")
	if err := task1(activityService, userService, trackpointService); err != nil {
		return err
	}

	fmt.Println("------------------")
	fmt.Println("      Task 2      ")
	fmt.Println("------------------")

	if err := task2(activityService); err != nil {
		return err
	}
	fmt.Println("------------------")
	fmt.Println("      Task 3      ")
	fmt.Println("------------------")
	if err := task3(activityService); err != nil {
		return err
	}

	fmt.Println("------------------")
	fmt.Println("      Task 4      ")
	fmt.Println("------------------")
	if err := task4(userService); err != nil {
		return err
	}

	fmt.Println("------------------")
	fmt.Println("      Task 5      ")
	fmt.Println("------------------")
	if err := task5(activityService); err != nil {
		return err
	}

	fmt.Println("------------------")
	fmt.Println("      Task 6      ")
	fmt.Println("------------------")
	if err := task6(activityService); err != nil {
		return err
	}

	fmt.Println("------------------")
	fmt.Println("      Task 7      ")
	fmt.Println("------------------")
	if err := task7(activityService); err != nil {
		return err
	}

	fmt.Println("------------------")
	fmt.Println("      Task 8      ")
	fmt.Println("------------------")
	if err := task8(userService); err != nil {
		return err
	}

	fmt.Println("------------------")
	fmt.Println("      Task 9      ")
	fmt.Println("------------------")
	if err := task9(userService); err != nil {
		return err
	}

	fmt.Println("------------------")
	fmt.Println("      Task 10      ")
	fmt.Println("------------------")
	if err := task10(userService); err != nil {
		return err
	}

	fmt.Println("------------------")
	fmt.Println("      Task 11      ")
	fmt.Println("------------------")
	if err := task11(activityService); err != nil {
		return err
	}
	return nil
}
