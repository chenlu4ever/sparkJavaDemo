package com.sparksql;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * Sparkjdbc
 * 可以直接操作关系型数据库
 * 测试通过
 * */
public class Sparkjdbc {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder().appName("sparkSQL").master("local").enableHiveSupport().getOrCreate();
        String url = "jdbc:hive2://192.168.29.128:10000/bustest";
        String table = "student_tbl";
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "root");
        properties.setProperty("driver", "org.apache.hive.jdbc.HiveDriver");
        try {
            spark.read().jdbc(url,table,properties).createTempView("student_tbl");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
        spark.close();
    }
}
