import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * SparkSql
 * 测试通过
 */
public class SparkSql {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder().appName("sparkSQL")
                .master("local")
                .enableHiveSupport().getOrCreate();
//        spark.sql("show tables").show();
        spark.sql("select id,name from bustest.student_tbl limit 10").show();
        Dataset<Row> ds =  spark.sql("select id,name from bustest.student_tbl limit 10");
        JavaRDD rdd =ds.javaRDD();
        System.out.println("统计RDD的所有元素:" + rdd.count());
        System.out.println("每个元素出现的次数:" + rdd.countByValue());
        System.out.println("取出rdd返回2个元素:" + rdd.take(2));
        System.out.println("取出rdd返回最前2个元素:" + rdd.top(2));
//        rdd.foreach(t -> System.out.print(t));
        spark.close();
    }
}
