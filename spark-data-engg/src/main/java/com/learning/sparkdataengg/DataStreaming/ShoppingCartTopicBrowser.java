package com.learning.sparkdataengg.chapter4;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.CountDownLatch;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class ShoppingCartTopicBrowser {

    public static void main(String[] args) {

        System.out.println("******** Shopping Carts abandoned - topic browser *************");

        //Needed for windows only
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

        Logger.getLogger("org").setLevel(Level.WARN );
        Logger.getLogger("akka").setLevel(Level.WARN);
        try {

            //Create the Spark Session
            SparkSession spark = SparkSession
                    .builder()
                    .master("local[2]")
                    .config("spark.sql.shuffle.partitions",2)
                    .config("spark.default.parallelism",2)
                    .config("spark.driver.host","127.0.0.1")
                    .config("spark.driver.bindAddress","127.0.0.1")
                    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
                    .appName("ShoppingCardsAbandonedBrowser")
                    .getOrCreate();

            System.out.println("Reading from Kafka..");

            //Consume the website visits topic
            Dataset<Row> shoppingCardsDf = spark
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", "spark.streaming.carts.abandoned")
                    .option("startingOffsets","earliest")
                    .load();

            shoppingCardsDf
                    .selectExpr("CAST(value AS STRING) as value")
                    .writeStream()
                    .foreach(
                        new ForeachWriter<Row>() {

                            @Override public boolean open(long partitionId, long version) {
                                return true;
                            }
                            @Override public void process(Row record) {
                                System.out.println("Retrieved Shopping Card Record "
                                        + " : " + record.toString() );
                            }
                            @Override public void close(Throwable errorOrNull) {
                                // Close the connection
                            }
                        }
                    )
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
