package org.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);
    public static void main(String[] args) {
        log.info("Hello world");

        //create producer properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        // create producer

        KafkaProducer<String,String> producer = new KafkaProducer<>(prop);

            //create producecr record
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>("java_demo","hello world , am a backend engineer now with kafka ");

            //send data
            producer.send(producerRecord);

            //flush and close the producer
            producer.flush();
            producer.close();
        
    }
}
