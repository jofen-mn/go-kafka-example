package main

import (
    "sarama"
    "github.com/Sirupsen/logrus"
    "fmt"
    "encoding/json"
)

func main() {
    config := sarama.NewConfig()
    config.Producer.RequiredAcks = sarama.WaitForAll
    config.Producer.Partitioner = sarama.NewRandomPartitioner
    config.Producer.Return.Successes = true

    addr := []string{"10.10.100.21:9092"}

    // 创建生产者
    producer, err := sarama.NewSyncProducer(addr, config)
    if err != nil {
        logrus.WithFields(logrus.Fields{
            "Module": "Create producer",
            "Error": err,
        })
    }

    defer producer.Close()

    msg := &sarama.ProducerMessage{
        Topic: "tp_helloworldjinfajinfa",
        Partition: int32(0),
        Key: sarama.StringEncoder("key"),
    }

    //var value string
    var (
        v1 string
        v2 string
        v3 string
    )

    for {

        _, err := fmt.Scanf("%s", &v1)
        if err != nil {
            break
        }

        _, err = fmt.Scanf("%s", &v2)
        if err != nil {
            break
        }

        _, err = fmt.Scanf("%s", &v3)
        if err != nil {
            break
        }

        valueTmp := struct {
            Value1 string
            Value2 string
            Value3 string
        }{v1, v2, v3}

        value, err := json.Marshal(valueTmp)
        if err != nil {
            logrus.WithFields(
                logrus.Fields{
                    "err": err,
                    "message": "send msg failed.",
                }).Error(err)
            return
        }

        msg.Value = sarama.ByteEncoder(string(value))
        fmt.Println(string(value))

        partition, offset, err := producer.SendMessage(msg)
        if err != nil {
            logrus.WithFields(logrus.Fields{
                "err": err,
                "message": "send msg failed.",
            })
            return
        }
        logrus.WithFields(logrus.Fields{
            "partition": partition,
            "offset": offset,
        })
    }
}
