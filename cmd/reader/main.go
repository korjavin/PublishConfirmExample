package main

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

func main() {
	// Connect to the RabbitMQ server
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Error connecting to RabbitMQ: %v", err)
	}
	defer conn.Close()

	// Open a channel on the connection
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Error opening RabbitMQ channel: %v", err)
	}
	defer ch.Close()

	// Consume messages from the queue
	msgs, err := ch.Consume(
		"messages", // Name of the queue
		"",         // Consumer tag
		true,       // Auto-acknowledge messages
		false,      // Exclusive
		false,      // No-local
		false,      // No-wait
		nil,        // Arguments
	)
	if err != nil {
		log.Fatalf("Error consuming RabbitMQ messages: %v", err)
	}

	// Print each message to stdout
	for msg := range msgs {
		fmt.Println(string(msg.Body))
	}
}
