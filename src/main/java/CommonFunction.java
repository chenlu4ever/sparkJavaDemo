import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

public class CommonFunction {
    public static void printForeach(JavaRDD rdd) {
        rdd.foreach(new VoidFunction<Object>() {
            public void call(Object val) throws Exception {
                System.out.println(val);
            }
        });
    }
}
