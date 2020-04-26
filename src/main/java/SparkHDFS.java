import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

/**
 * spark直接操作HDFS
 * 测试通过
 */
public class SparkHDFS {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder().appName("sparkSQL").master("local").enableHiveSupport().getOrCreate();
        JavaSparkContext javaSparkContext=new JavaSparkContext(spark.sparkContext());
        //book
        JavaRDD<String> javaRDD=javaSparkContext.textFile("hdfs://192.168.29.128:9000//user/hive/warehouse/bustest.db/book_tbl/book_tbl.dat");
        javaRDD.foreach(new VoidFunction<String>() {
            public void call(String line) throws Exception {
                System.out.println(line);
                System.out.println("------------"+line);
            }
        });
    }
}
