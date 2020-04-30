package com.ml;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

/**
 * KMeans.train 参数：
 * k 表示期望的聚类的个数。
 * maxInterations 表示方法单次运行最大的迭代次数。
 * runs 表示算法被运行的次数。K-means 算法不保证能返回全局最优的聚类结果，所以在目标数据集上多次跑 K-means 算法，有助于返回最佳聚类结果。
 * initializationMode 表示初始聚类中心点的选择方式, 目前支持随机选择或者 K-means||方式。默认是 K-means||。
 * initializationSteps表示 K-means||方法中的部数。
 * epsilon 表示 K-means 算法迭代收敛的阀值。
 * seed 表示集群初始化时的随机种子。
 */
public class FootballKMeans {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("sparkSQL")
                .master("local")
                .getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        javaSparkContext.setLogLevel("WARN");

        String fileUrl = "D:/IdeaProjects/sparkJavaDemo/src/main/resources/mlfootball/";

        JavaRDD<String> data = javaSparkContext.textFile(fileUrl + "football.dat");

        final JavaRDD<Vector> dataV = data.map(new Function<String, Vector>() {
            public Vector call(String s) throws Exception {
                System.out.println("s = " + s);
                String[] fields = s.split(",");
                if (fields.length != 3) {
                    throw new IllegalArgumentException("每一行必须有且只有3个元素");
                }
                double[] ds = new double[fields.length];
                for (int i = 0; i < fields.length; i++) {
                    ds[i] = Double.valueOf(fields[i]);
                }
                return Vectors.dense(ds);
            }
        }).cache();
        final KMeansModel model = KMeans.train(dataV.rdd(), 3, 5);
        System.out.println("clusterCenters is: " + model.clusterCenters());

        model.computeCost(dataV.rdd());

        dataV.foreach(new VoidFunction<Vector>() {
            public void call(Vector vector) throws Exception {
                System.out.println(String.valueOf(vector)+"------------"+ model.predict(vector));
            }
        });
    }
}