package com.learning.sparkdataengg.chapter4;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

/****************************************************************************
 * This Class creates and manages connections to MariaDB with an
 * order_summary table. Its run() method prints the summary of the orders table at
 * periodic intervals. It also supports an insert function to insert
 * new records to the table for testing purposes
 ***************************************************************************
 **/

public class MariaDBManager implements Serializable {

    private Connection conn;

    public static void main(String[] args) {

        System.out.println("Starting MariaDB DB Manager");
        MariaDBManager sqldbm = new MariaDBManager();
        sqldbm.setUp();
        sqldbm.insertSummary("2020-08-20 00:00:00","Review",46);
    }

    public void setUp() {
        System.out.println("Setting up MariaDB Connection");
        String url = "jdbc:mysql://localhost:3306/website_stats";
        try {
            conn = DriverManager.getConnection(url,"spark","spark");
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void teardown() {
        try {
            conn.close();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    //Used by StreamingAnalytics for writing data into MariaDB
    public void insertSummary(String timestamp, String lastAction, long duration) {
        try{

            String sql = "INSERT INTO visit_stats "
                            + "(INTERVAL_TIMESTAMP, LAST_ACTION, DURATION) VALUES "
                            +"( '" + timestamp + "',"
                            +" '" + lastAction + "',"
                            + duration + ")";
            //System.out.println(sql);
            conn.createStatement().execute(sql);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

}
