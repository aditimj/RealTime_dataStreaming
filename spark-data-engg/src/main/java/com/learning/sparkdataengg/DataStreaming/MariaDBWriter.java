package com.learning.sparkdataengg.chapter4;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

/*** This class is used to write to MySQL from inside the Flink ForeachWriter
 * operator. Note that each instance of the Flink Task will create its own connection.
 */
public class MariaDBWriter extends ForeachWriter<Row> {


    private static MariaDBManager updateMgr = null;

    @Override public boolean open(long partitionId, long version) {
        // Open connection
        if ( updateMgr == null) {
            updateMgr = new MariaDBManager();
            updateMgr.setUp();
        }
        return true;
    }
    @Override public void process(Row record) {
        // insert into DB
        updateMgr.insertSummary(
                ((GenericRowWithSchema)record.get(0)).get(0).toString(),
                record.getString(1),
                record.getLong(2) );

        System.out.println("Saving  Summary " + record.toString() );
    }

    @Override public void close(Throwable errorOrNull) {
    }
}
