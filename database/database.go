package database

import (
	"context"
	"log"
	"time"

	"github.com/diegolopezcode/Graphql-MongoDb-GO/graph/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var connectionString string = "mongodb://root:secret@localhost:27017"

type DB struct {
	client *mongo.Client
}

func Connect() *DB {
	client, err := mongo.NewClient(options.Client().ApplyURI(connectionString))
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatal(err)
	}

	return &DB{client: client}
}

func (db *DB) GetJob(id string) *model.JobListing {
	collection := db.client.Database("jobsDB").Collection("jobListings")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_id, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": _id}
	var job model.JobListing
	err := collection.FindOne(ctx, filter).Decode(&job)
	if err != nil {
		log.Fatal(err)
	}
	return &job
}

func (db *DB) GetJobListings() []*model.JobListing {
	collection := db.client.Database("jobsDB").Collection("jobListings")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var jobs []*model.JobListing
	cursor, err := collection.Find(ctx, model.JobListing{})
	if err != nil {
		log.Fatal(err)
	}
	err = cursor.All(context.TODO(), &jobs)
	if err != nil {
		log.Fatal(err)
	}
	return jobs
}

func (db *DB) CreateJobListing(input model.CreateJobListingInput) *model.JobListing {
	collection := db.client.Database("jobsDB").Collection("jobListings")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	job := model.JobListing{
		Title:       input.Title,
		Description: input.Description,
		Company:     input.Company,
		URL:         input.URL,
	}
	inserted, err := collection.InsertOne(ctx, job)
	if err != nil {
		log.Fatal(err)
	}
	job.ID = inserted.InsertedID.(primitive.ObjectID).Hex()

	return &job
}

func (db *DB) UpdateJobListing(id string, input model.UpdateJobListingInput) *model.JobListing {
	collection := db.client.Database("jobsDB").Collection("jobListings")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var job model.JobListing
	err := collection.FindOne(ctx, model.JobListing{ID: id}).Decode(&job)
	if err != nil {
		log.Fatal(err)
	}
	if input.Title != nil {
		job.Title = *input.Title
	}
	if input.Description != nil {
		job.Description = *input.Description
	}
	if input.URL != nil {
		job.URL = *input.URL
	}

	_id, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": _id}
	_, err = collection.UpdateOne(ctx, filter, bson.M{"$set": job})
	if err != nil {
		log.Fatal(err)
	}
	return &job
}

func (db *DB) DeleteJobListing(id string) *model.DeleteJobResponse {
	collection := db.client.Database("jobsDB").Collection("jobListings")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_id, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": _id}
	_, err := collection.DeleteOne(ctx, filter)
	if err != nil {
		log.Fatal(err)
	}
	return &model.DeleteJobResponse{ID: id, Success: true}
}
