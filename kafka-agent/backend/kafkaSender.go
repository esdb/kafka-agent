package backend

import (
	"fmt"
	"github.com/Shopify/sarama"
)

func SendToKafka() {
	messageSet := &sarama.MessageSet{
		Messages: []*sarama.MessageBlock{
			&sarama.MessageBlock{
				Msg: &sarama.Message{
					Value: []byte("hello"),
				},
			},
		},
	}
	request := &sarama.ProduceRequest{}
	request.AddSet("topic1", 0, messageSet)
	broker := sarama.NewBroker("127.0.0.1:1234")
	err := broker.Open(sarama.NewConfig())
	fmt.Println(err)
	_, err = broker.Produce(request)
	fmt.Println(err)
}