package com.sparksql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * SparkJavaRDD
 * 测试通过
 */
public class SparkJavaRDD {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder().appName("sparkSQL")
                .master("local")
                .enableHiveSupport().getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(data);
        System.out.println("-------------data print:---------------------");
        printForeach(javaRDD);

        System.out.println("-------------data X 2 print---------------------");
        //使用map算子，将集合中的每个元素都乘以2
        JavaRDD<Integer> multipleRDD = javaRDD.map(new Function<Integer, Integer>() {
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });
        printForeach(multipleRDD);

        System.out.println("-------------data filter print---------------------");
        //对集合使用filter算子，过滤出集合中的偶数
        JavaRDD<Integer> filterRDD = javaRDD.filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer integer) throws Exception {
                return integer%2==0;
            }
        });
        printForeach(filterRDD);
    }

    public static void printForeach(JavaRDD rdd) {
        rdd.foreach(new VoidFunction<Object>() {
            public void call(Object val) throws Exception {
                System.out.println(val);
            }
        });
    }


}
