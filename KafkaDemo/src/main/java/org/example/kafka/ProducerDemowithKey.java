package org.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemowithKey {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemowithKey.class);
    public static void main(String[] args) {
        log.info("Hello world");

        //create producer properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        // create producer

        KafkaProducer<String,String> producer = new KafkaProducer<>(prop);


        for (int i=0;i<10 ;i++){
            String topic = "java_demo";
            String key = "id_"+i;
            String value = "hello world"+i;

            //create producecr record
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic,key,value);


            //send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        log.info("received new metadata / \n"+
                                "Topic" + recordMetadata.topic() + "\n"+
                                "partition"+ recordMetadata.partition() + "\n"+
                                "key"+ producerRecord.key() + "\n"+
                                "offset"+ recordMetadata.offset() + "\n"+
                                "Timestamp" + recordMetadata.timestamp() );
                    }else{
                        log.error("something went wrong while producing " + e);
                    }
                }
            });
        }

        //flush and close the producer
        producer.flush();
        producer.close();

    }
}
