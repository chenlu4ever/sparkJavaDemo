package com.ml.classification;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TextNaiveBayes {
    public static Map<String, Integer> tag_map = new HashMap(){{
        put("社会",0);
        put("娱乐",1);
        put("时政",2);
        put("民生",3);
        put("科技",4);
        put("军事",5);
        put("天气",6);
        put("体育",7);
        put("房产",8);
        put("财经",9);
        put("汽车",10);
        put("情感",11);
        put("国际",12);
        put("育儿",13);
        put("文化",14);
        put("健康",15);
        put("游戏",16);
        put("教育",17);
        put("动漫",18);
        put("家居",19);
        put("农业",20);
        put("心灵鸡汤",21);
        put("科学",22);
        put("生活百科",23);
        put("时尚",24);
        put("旅行",25);
        put("彩票",26);
        put("星座",27);
        put("历史",28);
        put("美食",29);
        put("宗教",30);
        put("摄影",31);
        put("宠物",32);
        put("休闲",33);
        put("美女",34);
    }};
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("TextNaiveBayes")
                .master("local")
                .getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        javaSparkContext.setLogLevel("WARN");
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        String url = "src/main/resources/mldemo/sample_libsvm_data.txt";
        JavaRDD<Article> ratingsRDD = spark.read().textFile(url).javaRDD().map(Article::parseText);
        Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Article.class);
        Dataset<Row> df =ratings.toDF("category","label","text");

//        new Sentence2Words()
//        val wordsData =(df,spark).repartition(30);
//        wordsData.cache()
//        wordsData.count()
//
//        //求tf
//        val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
//        val featurizedData = hashingTF.transform(wordsData)
    }

    public static class Article implements Serializable {

        private static final long serialVersionUID = 1L;


        private String category;
        private int label;
        private String text;

        public Article() {
        }

        public Article(String category, int label, String text) {
            this.category = category;
            this.label = label;
            this.text = text;
        }

        public String getCategory() {
            return category;
        }

        public void setCategory(String category) {
            this.category = category;
        }

        public int getLabel() {
            return label;
        }

        public void setLabel(int label) {
            this.label = label;
        }

        public String getText() {
            return text;
        }

        public void setText(String text) {
            this.text = text;
        }

        public static Article parseText(String str) {
            String[] fields = str.split("\t");
            if (fields.length != 3) {
                throw new IllegalArgumentException("Each line must contain 3 fields");
            }
            String category = fields[0];
            int label = tag_map.get(fields[1]);
            String text = fields[2];
            return new Article(category, label, text );
        }
    }
}
