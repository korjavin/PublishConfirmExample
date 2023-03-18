package main

import (
	"database/sql"
	"fmt"
	"log"

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

	// Set publisher confirms on the channel
	confirmsChan := make(chan amqp.Confirmation, 10)
	// Enable publish confirmations
	if err := ch.Confirm(false); err != nil {
		close(confirmsChan)
		fmt.Printf("Channel could not be put into confirm mode: %s\n", err)
	}
	confirms := ch.NotifyPublish(confirmsChan)
	// defer confirmCleanup(ch, confirms)

	// Loop through the result set and publish each id to the RabbitMQ queue
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			log.Fatal(err)
		}

		// Publish the message to the RabbitMQ queue
		log.Printf("Sending message: %d to queue %s", id, q.Name)
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			true,   // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType:  "text/plain",
				Body:         []byte(fmt.Sprintf("%d", id)),
				DeliveryMode: amqp.Persistent,
			})
		if err != nil {
			log.Fatal(err)
		}

		// Wait for confirmation from the broker
		log.Printf("Waiting for confirmation...")
		confirm := <-confirms
		if !confirm.Ack {
			log.Fatalf("Failed to receive confirmation for message ID %d", id)
		}

		// Set the "is_sent" flag to true for the current id in the database
		_, err = db.Exec("UPDATE messages SET is_sent = true WHERE id = $1", id)
		if err != nil {
			log.Fatal(err)
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}

// Cleanup function to wait for confirmations before closing the channel
func confirmCleanup(ch *amqp.Channel, confirms <-chan amqp.Confirmation) {
	log.Printf("Waiting for publisher confirms...")
	for confirm := range confirms {
		if !confirm.Ack {
			log.Fatalf("Error delivering message: %v", confirm)
		}
	}
	log.Printf("All messages delivered successfully")
	ch.Close()
}
