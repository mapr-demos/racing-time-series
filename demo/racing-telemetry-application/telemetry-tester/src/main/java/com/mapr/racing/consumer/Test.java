package com.mapr.racing.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

/**
 * Simple class to test the consumer
 */
public class Test {

  public static void main(String[] args) {

    if (args.length == 0) {
      System.out.println( "Specify at least 1 topic to run the application" );
      System.exit(0);
    }

    KafkaConsumer<String, String> consumer = null;
    try{

      Properties properties = new Properties();
      properties.setProperty("group.id", "tester-"+ new Random().nextInt(100000));
      properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

      consumer = new KafkaConsumer<String,String>(properties);
      consumer.subscribe(Arrays.asList(args));
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(200);
        for (ConsumerRecord<String, String> record : records) {
          System.out.println(record);
        }
      }
    } finally {
      if (consumer != null) {
        consumer.close();
      }
    }


  }

}
