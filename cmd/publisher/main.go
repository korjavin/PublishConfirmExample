package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
	"github.com/samuel/go-zookeeper/zk"
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
	// Connect to ZooKeeper
	zkServers := []string{"127.0.0.1:2181"}
	zkconn, _, err := zk.Connect(zkServers, time.Second)
	if err != nil {
		log.Fatalf("Failed to connect to ZooKeeper: %v", err)
	}
	defer zkconn.Close()

	err = createAppTransactionNode(zkconn)
	if err != nil {
		log.Fatalf("Failed to create transaction node: %v", err)
	}

	// Connect to the Postgres database
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = ApplyPrepared(zkconn, db)
	if err != nil {
		log.Fatal(err)
	}
	// log.Fatal("Failed to apply prepared transactions")

	// Create a transaction node (ephemeral and sequential)
	transactionNode, err := createTransactionNode(zkconn)
	if err != nil {
		log.Fatalf("Failed to create transaction node: %v", err)
	}
	log.Printf("Created transaction node: %s", transactionNode)

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

		err := ch.Tx()
		if err != nil {
			log.Fatalf("Failed to begin a transaction: %v", err)
		}

		// Publish the message to the RabbitMQ queue
		log.Printf("Preparing message: %d to queue %s", id, q.Name)
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

		// if id > 2 {
		// 	log.Printf("I will sleep for 30 sec, please find a way to kill something to see if messages to rmq will be aborted as well")
		// 	time.Sleep(30 * time.Second)
		// }

		err = prepare(zkconn, transactionNode, db, id)
		if err != nil {
			log.Fatalf("Failed to prepare transaction: %v", err)
		}

		log.Printf("Committing transaction rmq message: %d", id)
		err = ch.TxCommit()
		if err != nil {
			log.Fatalf("Failed to commit rmq transaction: %v", err)
		}

		log.Printf("Committed transaction rmq  message: %d", id)

		// Commit the prepared transaction //TODO uncomment to test
		// if id > 2 {
		// 	log.Fatalf("AAAALLLLLAAARM")
		// }
		_, err = db.Exec(fmt.Sprintf("COMMIT PREPARED '%s'", transactionNode))
		if err != nil {
			log.Fatalf("Failed to commit transaction %s: %v", transactionNode, err)
		}
		log.Printf("Committed transaction: %s", transactionNode)
		zkconn.Delete(transactionNode, -1)

	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}

// Function read all the notex from the /tx- node and check if the transaction is prepared
// if prepared commit it to postgresql
func ApplyPrepared(conn *zk.Conn, db *sql.DB) error {
	children, _, err := conn.Children("/myapp")
	if err != nil {
		return err
	}

	for _, child := range children {
		log.Printf("Found transaction node: %d", child)
		// Check if the transaction is prepared
		// get content of the node with name /myapp/id
		_, _, err := conn.Get("/myapp/" + child)
		if err != nil {
			return err
		}
		log.Printf("Transaction %s is in zk, commiting it", "/myapp/"+child)

		// Commit the prepared transaction
		_, err = db.Exec(fmt.Sprintf("COMMIT PREPARED '%s'", "/myapp/"+child))
		if err != nil {
			log.Printf("Failed to commit transaction %s: %v", "/myapp/"+child, err)
		}
		log.Printf("Committed transaction	: %s", child)
		conn.Delete("myapp/"+child, -1)
	}
	return nil
}

func createAppTransactionNode(conn *zk.Conn) error {
	conn.Create("/myapp", nil, 0, zk.WorldACL(zk.PermAll))
	return nil
}

func createTransactionNode(conn *zk.Conn) (string, error) {
	return conn.Create("/myapp/tx-", nil, zk.FlagSequence, zk.WorldACL(zk.PermAll))
}

func prepare(conn *zk.Conn, transactionNode string, db *sql.DB, id int) error {
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// Set the "is_sent" flag to true for the current id in the database
	_, err = tx.Exec("UPDATE messages SET is_sent = true WHERE id = $1", id)
	if err != nil {
		log.Fatal(err)
	}

	// Prepare the transaction
	_, err = tx.Exec(fmt.Sprintf("PREPARE TRANSACTION '%s'", transactionNode))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Prepared transaction in pg: '%s'", transactionNode)

	// Create PostgreSQL participant znode in Zookeeper
	pgZNode := fmt.Sprintf("%s/%d", transactionNode, id)
	_, err = conn.Create(pgZNode, []byte("prepared"), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}

	return nil
}
