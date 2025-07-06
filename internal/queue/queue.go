package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/Saumya40-codes/LogsGO/pkg/store"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

func StartWorker(ctx context.Context, qConfig pkg.QueueConfig, store store.Store, workerID int) {
	cfg := qConfig.Queue
	conn, err := amqp.Dial(cfg.QueueURL)
	if err != nil {
		log.Println("Failed to open connection with queue")
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Println("Failed to open channel")
		return
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		cfg.QueueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Println("Failed to declare queue for a worker")
		return
	}

	logs, err := ch.Consume(
		q.Name,
		fmt.Sprintf("Log Queue Worker %d", workerID),
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Println("Failed to register the worker to the queue")
		return
	}

	go func() {
		for d := range logs {
			var batch logapi.LogBatch
			if err := proto.Unmarshal(d.Body, &batch); err != nil {
				log.Printf("Failed to unmarshal log batch: %v", err)
				continue
			}

			if err := store.Insert(batch.Entries, nil); err != nil {
				log.Printf("Failed to insert logs into store: %v", err)
			} else {
				log.Printf("Inserted %d logs from queue", len(batch.Entries))
			}
		}
	}()

	<-ctx.Done()
	log.Println("Worker shutting down gracefully.")
}

func ParseQueueConfig(path string) (*pkg.QueueConfig, error) {
	if path == "" {
		return nil, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("failed to open queue config file: " + err.Error())
	}
	defer file.Close()

	qCfg := &pkg.QueueConfig{}
	if err := yaml.NewDecoder(file).Decode(qCfg); err != nil {
		return nil, errors.New("failed to decode queue config: " + err.Error())
	}

	return qCfg, nil
}
