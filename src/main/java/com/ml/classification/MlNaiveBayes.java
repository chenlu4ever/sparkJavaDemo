package com.ml.classification;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 分类-朴素贝叶斯
 */
public class MlNaiveBayes {
    public static void main(String[] args) {
//        已配置环境变量HADOOP_HOME
//        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop3.0.0");

        SparkSession spark = SparkSession.builder().appName("Mllibsvm")
                .master("local")
                .getOrCreate();
//        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
//        javaSparkContext.setLogLevel("WARN");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        // 读取数据
//        String url = "src/main/resources/mldemo/mlscaler.txt";//一维特征
        String url = "src/main/resources/mldemo/sample_libsvm_data.txt";//多维特征
        Dataset<Row> dataset  = spark.read().format("libsvm").load(url);
        System.out.println("原始数据：----------------------------");
        dataset.show(10,false);
        System.out.println("");
        System.out.println("");


        // 将经过主成分分析的数据，按比例划分为训练数据和测试数据
        Dataset<Row>[] dsarray = dataset.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData =dsarray[0];
        Dataset<Row> testData=dsarray[1];

        NaiveBayes naiveBayes= new NaiveBayes().setSmoothing(1.0).setModelType("multinomial");
        Dataset<Row> predictions  = naiveBayes.fit(trainingData).transform(testData);

        // 计算精度,越接近1，准确率越高
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("准确率accuracy = " + accuracy);

        spark.close();
    }
}
