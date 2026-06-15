package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(state routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(state)
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
			_, err := gameState.CommandMove(input)
			if err != nil {
				fmt.Print("Invalid move command")
				continue
			}
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
