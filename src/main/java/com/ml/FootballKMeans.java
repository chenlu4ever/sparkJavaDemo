package com.ml;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;

import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.SparkSession;

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
        SparkSession spark = SparkSession.builder().appName("FootballKMeans")
                .master("local")
                .getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        javaSparkContext.setLogLevel("WARN");

        String fileUrl = "D:/IdeaProjects/sparkJavaDemo/src/main/resources/mlfootball/";

        JavaRDD<String> javaRDDdata = javaSparkContext.textFile(fileUrl + "football.dat");

        JavaRDD<Vector> dataV = javaRDDdata.map(new Function<String, Vector>() {
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
                //Vectors.dense密集向量
                return Vectors.dense(ds);
            }
        }).cache();

        KMeansModel model = KMeans.train(dataV.rdd(), 3, 100);
        System.out.println("clusterCenters is: " + model.clusterCenters());

        //KMeansModel类也提供了计算集合内误差平方和（Within Set Sum of Squared Error, WSSSE）的方法来度量聚类的有效性
        //在真实k值未知的情况下，该值的变化可以作为选取合适k值的一个重要参考：输出本次聚类操作的收敛性，此值越低越好。
        model.computeCost(dataV.rdd());

//        // 使用原数据进行交叉评估预测
//        JavaRDD<String> crossRes = javaRDDdata.map(new Function<Vector, String>() {
//            public String call(Vector v1) throws Exception {
//                int a = model.predict(v1);
//                return v1.toString() + "==>" + a;
//            }
//        });

//        // 打印预测结果
//        crossRes.foreach(new VoidFunction<String>() {
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });
    }
}