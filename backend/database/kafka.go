package database

import (
	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
	"log"
	"time"
)

func CreateKafkaTopic(topic string) error {
	// Set broker configuration
	broker := sarama.NewBroker(viper.GetStringSlice("kafka.nodes")[0])

	// Additional configurations. Check sarama doc for more info
	config := sarama.NewConfig()
	config.Version = sarama.V2_4_0_0

	// Open broker connection with configs defined above
	broker.Open(config)

	// check if the connection was OK
	connected, err := broker.Connected()
	if err != nil {
		log.Print(err.Error())
		return err
	}
	log.Print(connected)

	// Setup the Topic details in CreateTopicRequest struct
	topicDetail := &sarama.TopicDetail{}
	topicDetail.NumPartitions = int32(1)
	topicDetail.ReplicationFactor = int16(1)
	topicDetail.ConfigEntries = make(map[string]*string)

	topicDetails := make(map[string]*sarama.TopicDetail)
	topicDetails[topic] = topicDetail

	request := sarama.CreateTopicsRequest{
		Timeout:      time.Second * 15,
		TopicDetails: topicDetails,
	}

	// Send request to Broker
	response, err := broker.CreateTopics(&request)

	// handle errors if any
	if err != nil {
		log.Printf("%#v", &err)
		return err
	}
	t := response.TopicErrors
	for key, val := range t {
		log.Printf("Key is %s", key)
		log.Printf("Value is %#v", val.Err.Error())
		log.Printf("Value3 is %#v", val.ErrMsg)
	}
	log.Printf("the response is %#v", response)

	// close connection to broker
	broker.Close()
	return nil
}
