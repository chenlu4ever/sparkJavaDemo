package com.sparksql;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * SparkSql
 * 测试通过
 */
public class SparkSqlRdd {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("sparkSQL")
                .master("local")
                .enableHiveSupport().getOrCreate();
        Dataset<Row> ds = spark.sql("select id,name,male,grade from bustest.student_tbl limit 10");
        ds.show();
        JavaRDD javaRDD = ds.javaRDD();
        System.out.println("--------------------------------------");
        System.out.println("统计RDD的所有元素:" + javaRDD.count());
        System.out.println("每个元素出现的次数:" + javaRDD.countByValue());
        System.out.println("取出rdd返回2个元素:" + javaRDD.take(2));
//        System.out.println("取出rdd返回最前2个元素:" + javaRDD.top(2));
        System.out.println("--------------------------------------");
        spark.close();
    }
}
