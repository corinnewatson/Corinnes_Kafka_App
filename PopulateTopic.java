package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


// This class sends metadata to a Topic from a Producer
public class PopulateTopic {

    private static final Logger log = LoggerFactory.getLogger(PopulateTopic.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a kafka producer :-)");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //create  the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Will send metadata 10 times to topics
        for (int i = 0; i < 10; i++) {

            //create producer record
            // Uses a Sticky Partitioner to increase productivity
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("My Topic", "lulu rules " + i);

            // send data - asynchronous
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {

                    // exceutes every time a record is successfully sent, or else an exception is thrown
                    if (e == null) {
                        log.info("Received new meta data, populating topic..  \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        log.error("Error while producing: ", e);
                    }
                }
            });
        }


            //flush data - synchronous
            producer.flush();

            // close producer
            producer.close();
        }
    }

