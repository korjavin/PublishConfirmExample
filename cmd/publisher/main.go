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
		"messages", // queue name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
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

		// Publish the message to the RabbitMQ queue
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(fmt.Sprintf("%d", id)),
			})
		if err != nil {
			log.Fatal(err)
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
