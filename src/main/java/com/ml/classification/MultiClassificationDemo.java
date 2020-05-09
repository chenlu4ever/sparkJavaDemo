package com.ml.classification;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 分类算法
 * 决策树与随机森林对比
 */
public class MultiClassificationDemo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("ADPrediction")
                .master("local")
                .getOrCreate();

        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        String url = "src/main/resources/mldemo/mlmultclassify.txt";//多维特征
        Dataset<Row> data = spark.read().format("libsvm").load(url);

        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol("label")
                .setOutputCol("indexedLabel")
                .fit(data);

        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(4)
                .fit(data);

        Dataset<Row>[] dsarray = data.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData =dsarray[0];
        Dataset<Row> testData=dsarray[1];

        // Train a DecisionTree model. 决策树
        DecisionTreeClassifier decisionTree = new DecisionTreeClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures");

        //Random Forest Mdoel 随机森林
        RandomForestClassifier randomForest = new RandomForestClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures")
                .setNumTrees(10);


        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

        Dataset dtDs = new Pipeline().setStages(new PipelineStage[]{labelIndexer, featureIndexer, decisionTree, labelConverter}).fit(trainingData).transform(testData);
        Dataset rfDs = new Pipeline().setStages(new PipelineStage[]{labelIndexer, featureIndexer, randomForest, labelConverter}).fit(trainingData).transform(testData);

        dtDs.select("predictedLabel", "label", "features").show(10,false);
        rfDs.select("predictedLabel", "label", "features").show(10,false);


        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");

        System.out.println("decisionTree accuracy:" + evaluator.evaluate(dtDs));
        System.out.println("randomForest accuracy:" + evaluator.evaluate(rfDs));

        spark.close();
    }
}
