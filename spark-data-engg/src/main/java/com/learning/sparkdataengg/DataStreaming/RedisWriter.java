package com.learning.sparkdataengg.chapter4;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import redis.clients.jedis.Jedis;

public class RedisWriter extends ForeachWriter<Row> {


    private static Jedis jedis = null;
    private static final String lbKey= "country-stats";

    public static void setUp() {
        try{
            //Jedis running on localhost and port 6379
            jedis =new Jedis("localhost");
            //reset the sorted set key
            jedis.del(lbKey);
            System.out.println("Redis connection setup successfully");
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Override public boolean open(long partitionId, long version) {
        // Open connection
        if ( jedis == null) {
            jedis = new Jedis("localhost");
        }
        return true;
    }
    @Override public void process(Row record) {

        System.out.println("Retrieved Country Record " + record.toString() );

        // Update Redis SortedSet with incremental scores
        String country = record.getString(0);
        int increment = record.getInt(1);

        jedis.zincrby(lbKey,increment,country);

    }

    @Override public void close(Throwable errorOrNull) {
        // Close the connection
    }
}
