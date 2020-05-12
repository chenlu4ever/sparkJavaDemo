package com.ml.classification;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 分类：线性支持向量机
 *StandardScaler+PCA+LinearSVC
 * 二分类
 */
public class MlLinearSVC {
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

        // 数据预处理之标准归一化：标准化数据=(原数据-均值)/标准差
        //还有MinMaxScaler、MaxAbsScaler、Normalizer
        StandardScalerModel scaler = new StandardScaler()
               .setInputCol("features")
               .setOutputCol("scaledFeatures")
               .setWithMean(true)//默认为False。在缩放之前，将数据以均值居中。它将生成密集的输出，因此在应用于稀疏输入时要小心。将均值移到0，注意对于稀疏输入矩阵不可以用。默认为false
               .setWithStd(true)//默认为True。将数据缩放到单位标准偏差,将方差缩放到1。
               .fit(dataset);

//        Dataset<Row> scalerData = scaler.transform(dataset).select("label", "scaledFeatures").toDF("label","features");
        Dataset<Row> scalerData = scaler.transform(dataset);
        System.out.println("归一化数据：----------------------------");
        scalerData.show(10,false);
        System.out.println("");
        System.out.println("");

        // 创建PCA模型(主成分分析)，进行PCA降维 生成Transformer:
        PCAModel pca = new PCA()
                .setInputCol("features")
                .setOutputCol("pcaFeatures")
                .setK(5)
                .fit(scalerData);

        //transform 数据，生成主成分特征
        Dataset<Row> pcaResult = pca.transform(scalerData).select("label","pcaFeatures").toDF("label","features");
        System.out.println("PCA主成分分析数据：----------------------------");
        pcaResult.show(10,false);
        System.out.println("");
        System.out.println("");

        //  pcaResult.show(truncate=false)


//        // 这里主要是为了输出CVS文件格式转换 VectorAssembler（基础特征）将标签与主成分合成为一列
//        VectorAssembler assembler = new VectorAssembler()
//                .setInputCols(new String[]{"label","features"})
//                .setOutputCol("assemble");
//        Dataset<Row> output = assembler.transform(pcaResult);
//        System.out.println("VectorAssembler数据：----------------------------");
//        output.foreach(new ForeachFunction<Row>() {
//            public void call(Row row) throws Exception {
//                System.out.println("row = " + row.json());
//            }
//        });
//        System.out.println("");
//        System.out.println("");
//        // 输出csv格式的标签和主成分，便于可视化
//        Dataset<Row> ass = output.select(output.col("assemble").cast("string"));
//        ass.write().mode("overwrite").csv("src/main/resources/mldemo/output.csv");


        // 将经过主成分分析的数据，按比例划分为训练数据和测试数据
        Dataset<Row>[] dsarray = pcaResult.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData =dsarray[0];
        Dataset<Row> testData=dsarray[1];

        // 创建SVC分类器(Estimator)
        LinearSVC lsvc = new LinearSVC()
                .setMaxIter(10) //设置最大迭代次数
                .setRegParam(0.1);//设置正则化参数

        // 创建pipeline, 将上述步骤连接起来 setStages参数顺序很重要
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {scaler,pca,lsvc});

        // 使用串联好的模型在训练集上训练
        PipelineModel model = pipeline.fit(trainingData);

        // 在测试集上测试
        Dataset<Row> predictions = model.transform(testData).select("prediction","label","features");

        System.out.println("predictions数据：----------------------------");
        predictions.show(10,false);
        System.out.println("");
        System.out.println("");

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
