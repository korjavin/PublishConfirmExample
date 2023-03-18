package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/itimofeev/go-saga"
	_ "github.com/lib/pq"
	"github.com/streadway/amqp"
)

const (
	dbHost     = "localhost"
	dbPort     = "5432"
	dbUser     = "postgres"
	dbPassword = "password"
	dbName     = "mydb"
)

func main() {
	// Connect to the Postgres database
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Query the "messages" table for all rows where "is_sent" is false
	rows, err := db.Query("SELECT id FROM messages WHERE is_sent = false")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Connect to the RabbitMQ server
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Open a channel to the RabbitMQ server
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	// Declare a queue to which we will publish messages
	q, err := ch.QueueDeclare(
		"messages",
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,
		// amqp.Table{"x-confirm-true": true},
	)
	if err != nil {
		log.Fatal(err)
	}

	// Loop through the result set and publish each id to the RabbitMQ queue
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			log.Fatal(err)
		}
		message := fmt.Sprintf("Message %d", id)
		// Create a new saga
		s := saga.NewSaga("Sending message")

		// Add a step to update the is_sent flag in PostgreSQL
		s.AddStep(&saga.Step{
			Name: "Update is_sent flag",
			Func: func(ctx context.Context) error {
				return updateMessageAsSent(db, id)
			},
			CompensateFunc: func(ctx context.Context) error {
				return updateMessageAsNotSent(db, id)
			},
		})

		// Add a step to send the message to RabbitMQ
		s.AddStep(&saga.Step{
			Name: "Send message to RabbitMQ",
			Func: func(ctx context.Context) error {
				if id > 2 {
					log.Printf("I will sleep for 30 sec, please find a way to kill postgresql or/and rabbitmq to see if messages to rmq will be aborted as well")
					time.Sleep(30 * time.Second)
				}
				log.Printf("Sending message: %s to queue %s", message, q.Name)
				return publishMessage(ch, q.Name, message)
			},
			CompensateFunc: func(ctx context.Context) error { return nil },
		})

		// Execute the saga
		store := saga.New()
		c := saga.NewCoordinator(context.Background(), context.Background(), s, store)
		c.Play()
		if err != nil {
			log.Printf("Saga execution failed for message ID %d: %v", id, err)
		} else {
			log.Printf("Saga execution succeeded for message ID %d", id)
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func publishMessage(ch *amqp.Channel, queueName string, message string) error {
	return ch.Publish("", queueName, true, false, amqp.Publishing{
		ContentType:  "text/plain",
		Body:         []byte(message),
		DeliveryMode: amqp.Persistent,
	})
}

func updateMessageAsSent(db *sql.DB, id int) error {
	log.Printf("Updating message %d as sent", id)
	_, err := db.Exec("UPDATE messages SET is_sent = true WHERE id = $1", id)
	return err
}

func updateMessageAsNotSent(db *sql.DB, id int) error {
	log.Printf("Updating message %d as not sent", id)
	_, err := db.Exec("UPDATE messages SET is_sent = false WHERE id = $1", id)
	return err
}
