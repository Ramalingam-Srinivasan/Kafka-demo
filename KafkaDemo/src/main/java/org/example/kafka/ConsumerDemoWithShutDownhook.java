package org.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutDownhook {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutDownhook.class.getName());



    public static void main(String[] args) {
        log.info("am kafka application...");

        String bootStrapServer = "localhost:9092";
        String groupId = "my-third-application";
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

        //get reference to the current thread

        final Thread mainThread = Thread.currentThread();

        //adding shutdownhook

        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("detected a shutdown hook , lets exit by calling consumer wakeup");
                consumer.wakeup();

                //join the mainthread to allow the execution of the code in main thread
                try{
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try{
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
        catch (WakeupException we){
         log.info("wakeup exception");
         //lets ingnore this as this is an expected exception when closing a consumer
        }
        catch (Exception e){
           log.error("unexpected exception...");
        }
        finally {
            consumer.close(); // this will also commit the offset gracefully
            log.info("the consumer is gracefully closed ");
        }

    }

}
