package com.learning.sparkdataengg.chapter4;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/****************************************************************************
 * This Generator producers events about website visits by users
 * to a Kafka Topic
 ****************************************************************************/

public class WebsiteVisitsDataGenerator implements Runnable {

    public static final String topic = "spark.streaming.website.visits";

    public static void main(String[] args) {
        WebsiteVisitsDataGenerator wvdg = new WebsiteVisitsDataGenerator();
        wvdg.run();
    }

    public void run() {

        try {

            System.out.println("Starting Kafka Website Visits Generator..");
            //Wait for the main flow to be setup.
            Thread.sleep(5000);

            //Setup Kafka Client
            Properties kafkaProps = new Properties();
            kafkaProps.put("bootstrap.servers","localhost:9092");

            kafkaProps.put("key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put("value.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String,String> myProducer
                    = new KafkaProducer<String, String>(kafkaProps);

            //Define list of countries
            List<String> countries = new ArrayList<String>();
            countries.add("USA");
            countries.add("India");
            countries.add("Brazil");
            countries.add("Australia");
            countries.add("Russia");

            //Define list of last actions
            List<String> lastActions = new ArrayList<String>();
            lastActions.add("Catalog");
            lastActions.add("FAQ");
            lastActions.add("Order");
            lastActions.add("ShoppingCart");

            //Define a random number generator
            Random random = new Random();



            //Generate 100 sample visit records
            for(int i=0; i < 100; i++) {

                 //Generate a random Website Visit
                String country =countries.get(random.nextInt(countries.size()));
                String lastAction = lastActions.get(random.nextInt(lastActions.size()));
                String currentTime = String.valueOf(System.currentTimeMillis());
                int duration = random.nextInt(20) + 1;

                //Form a JSON. Use a dummy exception message
                String value= "{\"visitDate\":\"" + currentTime + "\","
                        +  "\"country\":\"" + country + "\","
                        +  "\"lastAction\":\"" + lastAction + "\","
                        +  "\"duration\":" + duration + "}" ;

                //Use country as key. Each country will go to the same partition
                //Hence the updates for a given country are sequencial
                String recKey = String.valueOf(country);


                ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>(
                                topic,
                                recKey,
                                value );

                RecordMetadata rmd = myProducer.send(record).get();

                System.out.println(
                            "Kafka Gaming Stream Generator : Sending Event : "
                            + recKey + " = " + value  );

                //Sleep for a random time ( 1 - 3 secs) before the next record.
                Thread.sleep(random.nextInt(2000) + 1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
