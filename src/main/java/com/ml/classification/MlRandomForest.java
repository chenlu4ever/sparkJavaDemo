package com.ml.classification;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 随机森林是决策树的组合算法，基础是决策树
 * 是解决分类和回归问题最为成功的机器学习算法之一
 */
public class MlRandomForest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("ADPrediction")
                .master("local")
                .getOrCreate();

        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        // Load the data stored in LIBSVM format as a DataFrame.
//        String url = "src/main/resources/mldemo/mlscaler.txt";//一维特征
        String url = "src/main/resources/mldemo/mlmultclassify.txt";//多维特征
        Dataset<Row> data = spark.read().format("libsvm").load(url);
        System.out.println("原始数据：------------------------");
        data.show(100,false);
        System.out.println("");
        System.out.println("");

        // Index labels, adding metadata to the label column.
        // Fit on whole dataset to include all labels in index.
        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol("label")
                .setOutputCol("indexedLabel")
                .fit(data);
        System.out.println("StringIndexer：出现次数最多为 0------------------------");
        labelIndexer.transform(data).show(10,false);
        System.out.println("");
        System.out.println("");

        // Automatically identify categorical features, and index them.
        //对数据集特征向量中的类别（离散值）特征（index categorical features categorical features ）进行编号
        //某列的特征取值范围大于setMaxCategories，则不转换。否则转。
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
                .fit(data);
        System.out.println("VectorIndexer：------------------------");
        featureIndexer.transform(data).show(10,false);
        System.out.println("");
        System.out.println("");

        //拆分数据为训练集和测试集（7:3）
        Dataset<Row>[] dsarray = data.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData =dsarray[0];
        Dataset<Row> testData=dsarray[1];
        testData.show(5,false);

        //创建模型
        RandomForestClassifier randomForest = new RandomForestClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures")
                .setNumTrees(10);

        //转化初始数据
        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

        //使用管道运行转换器和随机森林算法
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{labelIndexer, featureIndexer, randomForest, labelConverter});

        //训练模型
        PipelineModel model = pipeline.fit(trainingData);

        //预测
        Dataset predictions = model.transform(testData);

        // Select example rows to display.
        predictions.select("predictedLabel", "label", "features").show(10,false);

        //创建评估函数，计算错误率
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");

        double accuracy = evaluator.evaluate(predictions);
        System.out.println("accuracy = " + accuracy);

        spark.close();
    }
}
