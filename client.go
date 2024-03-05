package gomqttclient

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Config struct {
	Protocol string
	Broker string
	Port int
	Topic string
	Username string
	Password string
}

var cli mqtt.Client
var topic string

func InitClient(c Config) {
	topic = c.Topic

	connectAddress := fmt.Sprintf("%s://%s:%d", c.Protocol, c.Broker, c.Port)
	rand.New(rand.NewSource(time.Now().UnixNano()))
	clientID := fmt.Sprintf("go-client-%d", rand.Int())

	fmt.Println("connect address: ", connectAddress)
	opts := mqtt.NewClientOptions()
	opts.AddBroker(connectAddress)
	opts.SetUsername(c.Username)
	opts.SetPassword(c.Password)
	opts.SetClientID(clientID)
	opts.SetKeepAlive(time.Second * 60)

	cli = mqtt.NewClient(opts)
	token := cli.Connect()
	if token.WaitTimeout(time.Second*3) && token.Error() != nil {
		log.Fatal(token.Error())
	}
}

func Receive() <-chan string {
	ch := make(chan string)
	qos := 0
	cli.Subscribe(topic, byte(qos), func(cli mqtt.Client, msg mqtt.Message) {
		ch <- string(msg.Payload())
	})
	return ch
}

func Publish(payload string) {
	qos := 0
	if token := cli.Publish(topic, byte(qos), false, payload); token.Wait() && token.Error() != nil {
		fmt.Printf("publish failed, topic: %s, payload: %s\n", topic, payload)
	}
}
