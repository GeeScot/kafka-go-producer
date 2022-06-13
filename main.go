package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Measurement struct {
	MeasuredOn  string  `json:"measured_on"`
	Location    int     `json:"location"`
	Temperature float32 `json:"temperature"`
}

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "golang-kafka-producer",
		"acks":              "all",
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	// a, err := kafka.NewAdminClientFromProducer(p)
	// if err != nil {
	// 	fmt.Printf("Failed to create admin client: %s\n", err)
	// 	os.Exit(1)
	// }

	// result, err := a.CreatePartitions(context.Background(), []kafka.PartitionsSpecification{
	// 	{
	// 		Topic:      "measurements",
	// 		IncreaseTo: 10,
	// 	},
	// })

	// result, err := a.DeleteTopics(context.Background(), []string{"measurements"})
	// if err != nil {
	// 	fmt.Printf("Failed to create topic: %s\n", err)
	// 	os.Exit(1)
	// } else {
	// 	fmt.Printf("%v\n", result)
	// }

	// result, err = a.CreateTopics(context.Background(), []kafka.TopicSpecification{
	// 	{
	// 		Topic:             "measurements",
	// 		NumPartitions:     4,
	// 		ReplicationFactor: 1,
	// 	},
	// })

	// if err != nil {
	// 	fmt.Printf("Failed to create topic: %s\n", err)
	// 	os.Exit(1)
	// } else {
	// 	fmt.Printf("%v\n", result)
	// }

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	topic := "measurements"
	topicPartition := kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}

	numberOfBatches := 10
	batchSize := 100000

	start := time.Now()

	for batchNumber := 0; batchNumber < numberOfBatches; batchNumber++ {
		start := time.Now().UTC().Add(time.Duration(batchSize) * time.Minute)
		for i := 0; i < batchSize; i++ {
			b, err := json.Marshal(Measurement{
				MeasuredOn:  start.Add(time.Duration(i) * time.Minute).Format(time.RFC3339),
				Location:    1,
				Temperature: rand.Float32() * 24,
			})
			if err != nil {
				fmt.Printf("Failed to create marshal measurement: %s\n", err)
			}

			err = p.Produce(&kafka.Message{
				TopicPartition: topicPartition,
				Value:          b},
				nil,
			)
			if err != nil {
				fmt.Printf("Failed to produce measurement: %s\n", err)
			}
		}

		p.Flush(1000)
	}

	log.Printf("Finished sending %d events\n", numberOfBatches*batchSize)
	log.Printf("Start time: %v", start.Format(time.RFC3339))
	log.Printf("End time: %v", time.Now().Format(time.RFC3339))
}
