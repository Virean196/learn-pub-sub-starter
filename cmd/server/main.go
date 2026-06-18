package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	connectionString := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Unable to connect to RabbitMQ: %s", err)
	}
	defer connection.Close()
	fmt.Println("Connected to RabbitMQ successfully!")

	ch, err := connection.Channel()
	if err != nil {
		log.Fatalf("Failed to open Channel on connection: %s", err)
	}

	//	pubsub.DeclareAndBind(connection, routing.ExchangePerilTopic, routing.GameLogSlug,
	//	fmt.Sprintf("%s.*", routing.GameLogSlug), pubsub.SimpleQueueDurable)

	pubsub.SubscribeGob(connection, routing.ExchangePerilTopic, routing.GameLogSlug,
		fmt.Sprintf("%s.*", routing.GameLogSlug), pubsub.SimpleQueueDurable,
		func(log routing.GameLog) pubsub.AckType {
			defer fmt.Print("> ")
			err := gamelogic.WriteLog(log)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		})

	gamelogic.PrintClientHelp()
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			fmt.Print("Invalid username")
			continue
		}
		switch input[0] {
		case "pause":
			log.Print("Sending pause message")
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			continue
		case "resume":
			log.Print("Sending resume message")
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			continue
		case "quit":
			log.Print("Exiting")
			return
		default:
			log.Print("Invalid command, try again")
		}
	}
}
