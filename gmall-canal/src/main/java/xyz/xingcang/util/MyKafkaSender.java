package xyz.xingcang.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author xingcang
 * @create 2020-11-06 7:46 PM
 */
public class MyKafkaSender {
    public static KafkaProducer<String , String> kafkaProducer = null;

    public static KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop001:9092,hadoop002:9092,hadoop003:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return producer;
    }

    public static void Send(String topic, String msg) {
        if (kafkaProducer == null) {
            kafkaProducer = createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<>(topic, msg));
    }
}
