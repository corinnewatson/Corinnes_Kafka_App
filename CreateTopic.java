package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.util.Properties;

// This class creates a Topic and a Consumer
public class CreateTopic {

    private static final Logger log = LoggerFactory.getLogger(CreateTopic.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a kafka producer :-)");

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("My Topic", "lulu rules");


        // send data - asynchronous
        producer.send(producerRecord);


        //flush data - synchronous
        producer.flush();

        // close producer
        producer.close();
    }
}
