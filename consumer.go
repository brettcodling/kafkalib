package kafkalib

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/riferrei/srclient"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var schemaRegistryClient *srclient.SchemaRegistryClient
var schemaRegistryUrl string

// Consume will consume messages from a topic.
//
// Errors while consuming will be printed
// The messages will then be passed into `f`
// If `f` throws a panic it will be caught and handled
//
func Consume(f func(*kafka.Message)) {
	err := godotenv.Load()
	if err != nil {
		log.Fatal(err)
	}

	MaxPollInt := os.Getenv("KAFKA_MAX_POLL_INTERVAL")
	if MaxPollInt == "" {
		MaxPollInt = "300000"
	}

	SessTimeout := os.Getenv("KAFKA_SESSION_TIMEOUT_MS")
	if SessTimeout == "" {
		SessTimeout = "30000"
	}

	AutoCommitInt := os.Getenv("KAFKA_AUTO_COMMIT_INTERVAL_MS")
	if AutoCommitInt == "" {
		AutoCommitInt = "5000"
	}

	cm := kafka.ConfigMap{
		"bootstrap.servers":       os.Getenv("KAFKA_BROKER_URL"),
		"security.protocol":       os.Getenv("KAFKA_BROKER_SECURITY_PROTOCOL"),
		"sasl.mechanism":          os.Getenv("KAFKA_BROKER_SASL_MECHANISM"),
		"sasl.username":           os.Getenv("KAFKA_BROKER_SASL_USERNAME"),
		"sasl.password":           os.Getenv("KAFKA_BROKER_SASL_PASSWORD"),
		"max.poll.interval.ms":    MaxPollInt,
		"session.timeout.ms":      SessTimeout,
		"auto.commit.interval.ms": AutoCommitInt,
		"auto.offset.reset":       "latest",
		"group.id":                "default",
	}

	GroupId := os.Getenv("KAFKA_CONSUMER_GROUP_ID")
	if GroupId != "" {
		cm.SetKey("group.id", GroupId)
	}

	log.Println("Creating consumer")
	c, err := kafka.NewConsumer(&cm)
	log.Printf("brokerUrl: %s\n", cm["bootstrap.servers"])

	if err != nil {
		log.Fatal(err)
	}

	defer c.Close()

	c.SubscribeTopics([]string{os.Getenv("KAFKA_TOPIC")}, nil)

	log.Println("Subscribed to topics")

	schemaRegistryUrl = os.Getenv("KAFKA_SCHEMA_REGISTRY_URL")
	if schemaRegistryUrl != "" {
		log.Println("Creating schema registry client")
		schemaRegistryClient = srclient.CreateSchemaRegistryClient(os.Getenv("KAFKA_SCHEMA_REGISTRY_URL"))
		schemaRegistryUsername := os.Getenv("KAFKA_SCHEMA_REGISTRY_USERNAME")
		schemaRegistryPassword := os.Getenv("KAFKA_SCHEMA_REGISTRY_PASSWORD")
		if schemaRegistryUsername != "" && schemaRegistryPassword != "" {
			schemaRegistryClient.SetCredentials(schemaRegistryUsername, schemaRegistryPassword)
		}
	}

	log.Println("Listening for messages")

	for {
		msg, err := c.ReadMessage(-1)
		log.Println("New message received")
		if err != nil {
			// The client will automatically try to recover from all errors.
			log.Printf("Consumer error: %v (%v)\n", err, msg)
			continue
		}
		processMessage(msg, f)
	}
}

// decodeMessageValue will decode the kafka message value using the set up
// schema registry based on environment variables
func decodeMessageValue(msg *kafka.Message) {
	if cap(msg.Value) < 6 {
		log.Printf("Failed to get schema id from message: %s\n", string(msg.Value))
		return
	}
	schemaID := binary.BigEndian.Uint32(msg.Value[1:5])
	schema, err := schemaRegistryClient.GetSchema(int(schemaID))
	if err != nil {
		panic(fmt.Sprintf("Error getting the schema with id '%d' %s", schemaID, err))
	}
	native, _, _ := schema.Codec().NativeFromBinary(msg.Value[5:])
	value, _ := schema.Codec().TextualFromNative(nil, native)
	msg.Value = value
}

// processMessage will dispatch a worker that calls `f`
// It will also handle any panics that it throws
func processMessage(msg *kafka.Message, f func(*kafka.Message)) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Panic occurred:", err)
		}
	}()

	if schemaRegistryUrl != "" {
		decodeMessageValue(msg)
	}

	log.Println("Calling function with kafka message")
	go f(msg)
}

// Parse the kafka message value from json to a struct
func ParseKafkaMsg(msg *kafka.Message, value interface{}) (interface{}, error) {
	err := json.Unmarshal(msg.Value, &value)

	return value, err
}
