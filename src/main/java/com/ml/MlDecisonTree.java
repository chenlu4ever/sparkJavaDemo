package com.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 决策树分类(支持多种分类)
 *StringIndexer+VectorIndexer+DecisionTreeClassifier+IndexToString
 */
public class MlDecisonTree {
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

        // Split the data into training and test sets (30% held out for testing).
        Dataset<Row>[] dsarray = data.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData =dsarray[0];
        Dataset<Row> testData=dsarray[1];

        // Train a DecisionTree model. 决策树
        DecisionTreeClassifier dt = new DecisionTreeClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures");


        // Convert indexed labels back to original labels.
        //相应的，有StringIndexer，就应该有IndexToString。在应用StringIndexer对labels进行重新编号后，带着这些编号后的label对数据进行了训练，
        // 并接着对其他数据进行了预测，得到预测结果，预测结果的label也是重新编号过的，因此需要转换回来。见下面例子，转换回来的convetedPrediction才和原始的label对应。
        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

        // Chain indexers and tree in a Pipeline.
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{labelIndexer, featureIndexer, dt, labelConverter});

        // Train model. This also runs the indexers.
        PipelineModel model = pipeline.fit(trainingData);

        // Make predictions.
        Dataset predictions = model.transform(testData);

        // Select example rows to display.
        predictions.select("predictedLabel", "label", "features").show(10,false);

        // Select (prediction, true label) and compute test error.
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("accuracy = " + accuracy);

        spark.close();
    }
}
