import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

/**
 * 测试通过
 */
public class KafkaConsumer {
    public static void main(String[] args) throws InterruptedException {
        String brokers = "hadoop:9092";
        String topics = "test";
        SparkSession spark=SparkSession.builder().appName("sparkSQL").master("local").enableHiveSupport().getOrCreate();
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        sc.setLogLevel("WARN");
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));

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
        //这里就跟之前的demo一样了，只是需要注意这边的lines里的参数本身是个ConsumerRecord对象
//        JavaPairDStream<String, Integer> counts =
//                lines.flatMap(x -> Arrays.asList(x.value().toString().split(" ")).iterator())
//                        .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
//                        .reduceByKey((x, y) -> x + y);
//        counts.print();
//  可以打印所有信息，看下ConsumerRecord的结构
        lines.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<Object, Object>>>() {
            public void call(JavaRDD<ConsumerRecord<Object, Object>> consumerRecordJavaRDD) throws Exception {
                consumerRecordJavaRDD.foreach(new VoidFunction<ConsumerRecord<Object, Object>>() {
                    public void call(ConsumerRecord<Object, Object> record) throws Exception {
                        System.out.println("==== topic: " + record.topic()+",value:"+record.value());
                    }
                });
            }
        });
        ssc.start();
        ssc.awaitTermination();
        ssc.close();
        spark.close();

    }
}
