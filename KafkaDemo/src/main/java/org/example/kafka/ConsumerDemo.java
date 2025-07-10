package org.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getName());



    public static void main(String[] args) {
        log.info("am kafka application...");

        String bootStrapServer = "localhost:9092";
        String groupId = "my-second-application";
        String topic = "java_demo";

        //creating consumer configs
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(prop);

        //subscribe consumer
        consumer.subscribe(Arrays.asList(topic));

        //poll consumer

        while(true){

            log.info("polling...");
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record : records){
                log.info("key:"+record.key()+".....value:"+record.value());
                log.info("partition:"+record.partition()+".........offset:"+record.offset());
            }
        }

    }

}
