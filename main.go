package main

import (
	"fmt"
	"log"
	"runtime"
	"strconv"
	"sync"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	const threads = 16
	const messages = 1
	const extraLogging = false
	const sleepBetweenCycles = 50 * time.Millisecond
	var topic = "kafkaload"

	for runNo := 0; true; runNo++ {
		log.Printf("Starting run %d", runNo)

		// Creates several producers, publishes a single message, then shuts them down.
		wg := sync.WaitGroup{}
		wg.Add(threads)
		for i := 0; i < threads; i++ {
			i := i
			go func() {
				if extraLogging {
					log.Printf("Created producer [%d]", i)
				}
				prod, err := kafka.NewProducer(&kafka.ConfigMap{
					"bootstrap.servers":   "localhost:9092",
					"enable.idempotence":  true,
					"delivery.timeout.ms": 10000,
					"socket.timeout.ms":   1000,
					"debug":               "all",
				})
				if err != nil {
					panic(err)
				}
				defer prod.Close()

				go func() {
					for e := range prod.Events() {
						switch e.(type) {
						case *kafka.Message:
						default:
						}
					}
				}()

				if extraLogging {
					log.Printf("Published [%d]", i)
				}
				for m := 0; m < messages; m++ {
					err = prod.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &topic},
						Key:            []byte(strconv.Itoa(i)),
						Value:          []byte(strconv.FormatInt(time.Now().Unix(), 10)),
					}, nil)
					if err != nil {
						panic(err)
					}
				}
				wg.Done()
			}()
		}

		timer := time.NewTimer(60 * time.Second)
		done := make(chan int)

		// A watchdog that kicks in if Wait() took too long.
		go func() {
			select {
			case <-timer.C:
				dumpAllStacks()
				panic(fmt.Errorf("It happened"))
			case <-done:
			}
		}()

		wg.Wait()
		timer.Stop()
		close(done)

		if extraLogging {
			log.Printf("Done")
		}
		time.Sleep(sleepBetweenCycles)
	}
}

func dumpAllStacks() {
	bytes := make([]byte, 1<<20)
	len := runtime.Stack(bytes, true)
	fmt.Println(string(bytes[:len]))
}
