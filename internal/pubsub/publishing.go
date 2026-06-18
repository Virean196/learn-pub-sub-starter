package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

func PublishGameLog(ch *amqp.Channel, username, winner, loser string, outcome gamelogic.WarOutcome) error {
	var log routing.GameLog
	switch outcome {
	case gamelogic.WarOutcomeOpponentWon:
		log.Message = fmt.Sprintf("%s won a war against %s", winner, loser)
	case gamelogic.WarOutcomeYouWon:
		log.Message = fmt.Sprintf("%s won a war against %s", winner, loser)
	case gamelogic.WarOutcomeDraw:
		log.Message = fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
	}
	log.CurrentTime = time.Now()
	log.Username = username
	err := PublishGob(ch, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.GameLogSlug, username), log)
	if err != nil {
		return err
	}
	return nil
}

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	marshalledVal, err := json.Marshal(val)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false,
		amqp.Publishing{ContentType: "application/json", Body: marshalledVal})
	if err != nil {
		return err
	}
	return nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var b bytes.Buffer
	enconder := gob.NewEncoder(&b)
	err := enconder.Encode(val)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false,
		amqp.Publishing{ContentType: "application/gob", Body: b.Bytes()})
	if err != nil {
		return err
	}
	return nil
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	connCh, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	table := amqp.Table{}
	table["x-dead-letter-exchange"] = "peril_dlx"
	queue, err := connCh.QueueDeclare(queueName, queueType == SimpleQueueDurable,
		queueType == SimpleQueueTransient, queueType == SimpleQueueTransient, false, table)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	err = connCh.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	return connCh, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	deliveryChan, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for item := range deliveryChan {
			var target T
			err = json.Unmarshal(item.Body, &target)
			ack := handler(target)
			switch ack {
			case Ack:
				item.Ack(false)
				log.Print("Ack = false")
			case NackRequeue:
				item.Nack(false, true)
				log.Print("Nack = false, true")
			case NackDiscard:
				item.Nack(false, false)
				log.Print("Nack = false, false")
			}
		}
	}()
	return nil

}
func SubscribeGob[T any](
	conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	deliveryChan, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for item := range deliveryChan {
			var target T
			buf := bytes.NewBuffer(item.Body)
			decoder := gob.NewDecoder(buf)
			err := decoder.Decode(&target)
			if err != nil {
				log.Fatalf("Unable to decode message: %v", err)
			}
			ack := handler(target)
			switch ack {
			case Ack:
				item.Ack(false)
				log.Print("Ack = false")
			case NackRequeue:
				item.Nack(false, true)
				log.Print("Nack = false, true")
			case NackDiscard:
				item.Nack(false, false)
				log.Print("Nack = false, false")
			}

		}
	}()
	return nil
}
