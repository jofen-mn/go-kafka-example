package main

import (
    "sync"
    "sarama"
    "github.com/Sirupsen/logrus"
    "encoding/json"
)

var (
    wg sync.WaitGroup
    topic = "tp_helloworldjinfajinfa"
)

func main() {
    consumer, err := sarama.NewConsumer([]string{"10.10.100.21:9092"}, nil)
    if err != nil {
        logrus.WithFields(logrus.Fields{
            "err": err,
            "msg": "create consumer error.",
        })
        return
    }

    partitionList, err := consumer.Partitions(topic)
    if err != nil {
        logrus.WithFields(logrus.Fields{
            "err": err,
            "msg": "query partition list error.",
        })
        return
    }

    for _, partition := range partitionList {
        pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
        if err != nil {
            logrus.WithFields(logrus.Fields{
                "err": err,
                "msg": "get partition error.",
            })
            return
        }

        defer pc.AsyncClose()

        wg.Add(1)

        go func(partitionConsumer sarama.PartitionConsumer) {
            defer wg.Done()

            for msg := range pc.Messages() {
                logrus.Printf("Partition: %d, Offset:%d, Key:%s, Value:%s\n", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
                value := &struct {
                    Value1 string
                    Value2 string
                    Value3 string
                }{}
                err = json.Unmarshal(msg.Value, value)
                if err != nil {
                    logrus.WithFields(logrus.Fields{
                        "err": err,
                        "Msg": "unmarshal error.",
                    }).Error(err)
                }

                logrus.Printf("%+v", value)
            }
        }(pc)
    }

    wg.Wait()
    consumer.Close()
}