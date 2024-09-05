package com.learning.sparkdataengg.chapter4;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.types.DataTypes.*;

/****************************************************************************
 * This example is an example for Stream Processing in Spark.
 * It reads a real time website visits stream from kafka and
 * 1. Updates 5 second summaries in MariaDB
 * 2. Updates a Country scoreboard in Redis
 * 3. Updates a Kafka topic, when shopping cart is the last action.
 ****************************************************************************/
public class StreamingWebsiteAnalytics {

    public static void main(String[] args) {

        System.out.println("******** Initiating Streaming Website Analtyics *************");

        //Needed for windows only
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

        Logger.getLogger("org").setLevel(Level.WARN );
        Logger.getLogger("akka").setLevel(Level.WARN);

        //Initiate  Redis
        RedisWriter.setUp();

        //Initiate the Kafka Website Visits Generator
        WebsiteVisitsDataGenerator kafkaGenerator = new WebsiteVisitsDataGenerator();
        Thread genThread = new Thread(kafkaGenerator);
        genThread.start();

        System.out.println("******** Starting Streaming  *************");

        try {

            StructType schema = new StructType()
                    .add("visitDate", StringType)
                    .add("country", StringType)
                    .add("lastAction", StringType)
                    .add("duration", IntegerType );

            //Create the Spark Session
            SparkSession spark = SparkSession
                    .builder()
                    .master("local[2]")
                    .config("spark.driver.host","127.0.0.1")
                    .config("spark.driver.bindAddress","127.0.0.1")
                    .config("spark.sql.shuffle.partitions",2)
                    .config("spark.default.parallelism",2)
                    .appName("StreamingAnalyticsProcessor")
                    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
                    .getOrCreate();

            System.out.println("Reading from Kafka..");

            //Consume the website visits topic
            Dataset<Row> rawVisitsDf = spark
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", "spark.streaming.website.visits")
                    //.option("startingOffsets","earliest")
                    .load();

            Dataset<Row> visitsDf = rawVisitsDf
                    .selectExpr("CAST(value AS STRING) as value")
                    .select(functions.from_json(
                            functions.col("value"),schema).as("visits"))
                    .select("visits.visitDate",
                            "visits.country",
                            "visits.lastAction",
                            "visits.duration");

            visitsDf.printSchema();
            visitsDf
                    .writeStream()
                    .foreach(
                            new ForeachWriter<Row>() {

                                @Override public boolean open(long partitionId, long version) {
                                    return true;
                                }
                                @Override public void process(Row record) {
                                    System.out.println("Retrieved Visit Record "
                                            + " : " + record.toString() );
                                }
                                @Override public void close(Throwable errorOrNull) {
                                    // Close the connection
                                }
                            }
                    )
                    .start();

            //Filter all Last Action = Shopping Cart
            Dataset<Row> shoppingCartDf
                    = visitsDf
                        .filter( "lastAction == 'ShoppingCart'");

            //Write Abandoned Shopping Carts to an outgoing Kafka topic.
            shoppingCartDf
                    .selectExpr("format_string(\"%s,%s,%s,%d\", visitDate,country,lastAction,duration) as value")
                    .writeStream()
                    .format("kafka")
                    .option("checkpointLocation", "tmp/cp-shoppingcart2")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("topic", "spark.streaming.carts.abandoned")
                    .start();

            //Update country wise duration counters in real time
            visitsDf.select("country","duration")
                    .writeStream()
                    .foreach(new RedisWriter())
                    .start();

            //Create windows of 5 seconds and total Duration by Last Action
            Dataset<Row> windowedSummary = visitsDf
                    .withColumn("timestamp", functions.current_timestamp())
                    .withWatermark("timestamp","5 seconds")
                    .groupBy(functions.window(
                            functions.col("timestamp"),
                            "5 seconds"),
                            functions.col("lastAction"))
                    .agg(functions.sum(functions.col("duration")));

            windowedSummary
                    .writeStream()
                    .foreach(new MariaDBWriter())
                    .start();

            //Keep the process running
            final CountDownLatch latch = new CountDownLatch(1);
            latch.await();
        }
        catch(Exception e) {
            e.printStackTrace();
        }



    }
}
