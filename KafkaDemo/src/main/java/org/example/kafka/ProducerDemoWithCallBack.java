package org.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
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
            //create producecr record
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>("java_demo","hello world"+i);

            //send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        log.info("received new metadata / \n"+
                                "Topic" + recordMetadata.topic() + "\n"+
                                "partition"+ recordMetadata.partition() + "\n"+
                                "offset"+ recordMetadata.offset() + "\n"+
                                "Timestamp" + recordMetadata.timestamp() );
                    }else{
                        log.error("something went wrong while producing " + e);
                    }
                }
            });

            try{
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //flush and close the producer
        producer.flush();
        producer.close();

    }
}
