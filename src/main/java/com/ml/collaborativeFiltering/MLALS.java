package com.ml.collaborativeFiltering;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/***
 * 协同过滤算法
 * 电影打分推荐
 * 数据源
 * http://files.grouplens.org/datasets/movielens/ml-100k.zip
 */
public class MLALS {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("MovieRecommand")
                .master("local")
                .getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        javaSparkContext.setLogLevel("WARN");

        String fileUrl = "D:/IdeaProjects/sparkJavaDemo/src/main/resources/mlmovie/ratings.dat";

        JavaRDD<Rating> ratingsRDD = spark.read().textFile(fileUrl).javaRDD().map(Rating::parseRating);
        Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Rating.class);
        // 按比例随机拆分数据
        Dataset<Row>[] splits = ratings.randomSplit(new double[] { 0.8, 0.2 });
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];

        // 对训练数据集使用ALS算法构建建议模型
        ALS als = new ALS().setMaxIter(5) //要运行的最大迭代次数(默认值为10)
                .setRegParam(0.01) //指定的正则化参数(默认值为1.0)
                .setImplicitPrefs(false)//是否使用隐式反馈(默认为false，使用显式反馈)
                .setAlpha(1.0) //当使用隐式反馈时，用于控制偏好观察的基线置信度(默认值为1.0)
                .setUserCol("userId")
                .setItemCol("movieId")
                .setRatingCol("rating");
        ALSModel model = als.fit(training);

        // Evaluate the model by computing the RMSE on the test data
        // 通过计算均方根误差RMSE(Root Mean Squared Error)对测试数据集评估模型
        // 注意下面使用冷启动策略drop，确保不会有NaN评估指标
        model.setColdStartStrategy("drop");
        Dataset<Row> predictions = model.transform(test);

        // 打印predictions的schema
        predictions.printSchema();

        // predictions的schema输出
        // root
        // |-- movieId: integer (nullable = false)
        // |-- rating: float (nullable = false)
        // |-- timestamp: long (nullable = false)
        // |-- userId: integer (nullable = false)
        // |-- prediction: float (nullable = true)

        RegressionEvaluator evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating")
                .setPredictionCol("prediction");
        double rmse = evaluator.evaluate(predictions);
        // 打印均方根误差
        System.out.println("Root-mean-square error = " + rmse);

    }

    public static class Rating implements Serializable {

        private static final long serialVersionUID = 1L;
        private int userId;
        private int movieId;
        private float rating;
        private long timestamp;

        public Rating() {
        }

        public Rating(int userId, int movieId, float rating, long timestamp) {
            this.userId = userId;
            this.movieId = movieId;
            this.rating = rating;
            this.timestamp = timestamp;
        }

        public int getUserId() {
            return userId;
        }

        public int getMovieId() {
            return movieId;
        }

        public float getRating() {
            return rating;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public static Rating parseRating(String str) {
            String[] fields = str.split("::");
            if (fields.length != 4) {
                throw new IllegalArgumentException("Each line must contain 4 fields");
            }
            int userId = Integer.parseInt(fields[0]);
            int movieId = Integer.parseInt(fields[1]);
            float rating = Float.parseFloat(fields[2]);
            long timestamp = Long.parseLong(fields[3]);
            return new Rating(userId, movieId, rating, timestamp);
        }
    }

}
