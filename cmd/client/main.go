package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(state routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(state)
		return pubsub.Ack
	}
}

func main() {
	fmt.Println("Starting Peril client...")
	connectionString := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Unable to connect to RabbitMQ: %s", err)
	}
	defer connection.Close()

	ch, err := connection.Channel()
	if err != nil {
		log.Fatalf("could not open channel: %v", err)
	}
	username, err := gamelogic.ClientWelcome()

	if err != nil {
		fmt.Print(err)
	}
	gameState := gamelogic.NewGameState(username)

	queueName := fmt.Sprintf("pause.%s", username)
	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.SimpleQueueTransient, handlerPause(gameState))
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}

	pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, fmt.Sprintf("army_moves.%s", username), "army_moves.*", pubsub.SimpleQueueTransient,
		func(move gamelogic.ArmyMove) pubsub.AckType {
			moveOutcome := gameState.HandleMove(move)
			fmt.Print("> ")
			if moveOutcome == gamelogic.MoveOutComeSafe || moveOutcome == gamelogic.MoveOutcomeMakeWar {
				return pubsub.Ack
			}
			if moveOutcome == gamelogic.MoveOutcomeSamePlayer {
				return pubsub.NackDiscard
			}
			return pubsub.NackDiscard
		})
	for {
		input := gamelogic.GetInput()
		if len(input) == 2 {
			fmt.Print("Invalid spawn command, try <spawn location unit>")
			continue
		}
		switch input[0] {
		case "spawn":
			err := gameState.CommandSpawn(input)
			if err != nil {
				fmt.Print("Invalid spawn command")
				continue
			}
		case "move":
			move, err := gameState.CommandMove(input)
			if err != nil {
				fmt.Print("Invalid move command")
				continue
			}
			pubsub.PublishJSON(ch, routing.ExchangePerilTopic, fmt.Sprintf("army_moves.%s", username), move)
			fmt.Print("Move published successfully")
		case "status":
			gameState.CommandStatus()
			continue
		case "help":
			gamelogic.PrintClientHelp()
			continue
		case "spam":
			fmt.Print("Spamming now allowed yet!\n")
			continue
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Print("Invalid command, use <help>\n")
			continue
		}
		// Wait for os.Interrupt
		// 	signalCh := make(chan os.Signal, 1)
		// 	signal.Notify(signalCh, os.Interrupt)
		// 	<-signalCh
	}
}
