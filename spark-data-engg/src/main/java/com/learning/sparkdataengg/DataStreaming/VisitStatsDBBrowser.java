package com.learning.sparkdataengg.chapter4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class VisitStatsDBBrowser {

    public static void main(String[] args) {
        try {

            //Find latest ID
            int latestId=0;

            Class.forName("org.mariadb.jdbc.Driver");
            String url = "jdbc:mysql://localhost:3307/website_stats";
            Connection conn = DriverManager.getConnection(url,"spark","spark");

            String idQuery="SELECT IFNULL(MAX(ID),0) as LATEST_ID "
                    + "FROM visit_stats";
            ResultSet rsLatest = conn.createStatement().executeQuery(idQuery);

            while(rsLatest.next()) {
                latestId = rsLatest.getInt("LATEST_ID");
            }

            //SQL for periodic stats
            String selectSql = "SELECT count(*) as TotalRecords, "
                    + " sum(DURATION) as TotalDuration"
                    + " FROM visit_stats" ;
            System.out.println("Periodic Check Query : " + selectSql);

            while(true) {
                //Sleep for 5 seconds and then query summary
                Thread.sleep(5000);
                ResultSet rs = conn.createStatement().executeQuery(selectSql);

                while (rs.next()) {
                    System.out.println(
                            "DB Summary - since start : "
                            + "Records = " + rs.getInt("TotalRecords") + ", "
                            + "Duration = " + rs.getDouble("TotalDuration")
                            );
                }
            }



        } catch(Exception e) {
            e.printStackTrace();
        }

    }
}
