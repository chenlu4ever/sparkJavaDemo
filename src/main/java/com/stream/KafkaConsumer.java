package com.stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/**
 * 测试通过
 */
public class KafkaConsumer {
    public static void main(String[] args) throws InterruptedException {
        String brokers = "hadoop:9092";
        String topics = "test";
        // 该应用拥有的线程通过local[*]配置
        SparkSession spark=SparkSession.builder().appName("sparkSQL").master("local[2]").enableHiveSupport().getOrCreate();
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        sc.setLogLevel("WARN");
        // 其批处理时间间隔Durations为2s 多久触发一次job
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(2));

        Collection<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        //kafka相关参数，必要！缺了会报错
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("metadata.broker.list", brokers) ;
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", "SparkkafkaConsumer");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        //Topic分区
//        Map<TopicPartition, Long> offsets = new HashMap<TopicPartition, Long>();
//        offsets.put(new TopicPartition("topic1", 0), 2L);
        Collection<String> list = new ArrayList<String>();
        list.add(topics);
        JavaInputDStream<ConsumerRecord<Object,Object>> lines = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
//                ConsumerStrategies.Subscribe(topicsSet, kafkaParams, offsets)
                ConsumerStrategies.Subscribe(list,kafkaParams)
        );

        //可以打印所有信息，看下ConsumerRecord的结构
        lines.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<Object, Object>>>() {
            public void call(JavaRDD<ConsumerRecord<Object, Object>> consumerRecordJavaRDD) throws Exception {
                consumerRecordJavaRDD.foreach(new VoidFunction<ConsumerRecord<Object, Object>>() {
                    public void call(ConsumerRecord<Object, Object> record) throws Exception {
                        System.out.println("======== topic: " + record.topic()+",key:"+record.key()+",offset:"+record.offset()+",value:"+record.value());
                    }
                });
            }
        });

        // 启动流计算环境StreamingContext并等待它"完成",Spark Streaming应用启动之后是不能再添加业务逻辑
        ssc.start();
        // 等待作业完成
        ssc.awaitTermination();
        //无参的stop方法会将SparkContext一同关闭，解决办法：stop(false)
        ssc.close();
    }
}
