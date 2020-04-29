//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.RowFactory;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.Metadata;
//import org.apache.spark.sql.types.StructField;
//import org.apache.spark.sql.types.StructType;
//
//public class SparkSQLDemo {
//    public static void main(String[] args) {
//        SparkSession spark=SparkSession.builder().appName("sparkSQL").master("local").enableHiveSupport().getOrCreate();
//        //创建java spark上下文
//        JavaSparkContext javaSparkContext=new JavaSparkContext(spark.sparkContext());
//
//        //student
//        JavaRDD<String> s_rdd1=javaSparkContext.textFile("/user/hadoop/data2/customers.txt");
//        JavaRDD<Row> s_rdd2=s_rdd1.map(new Function<String, Row>() {
//            public Row call(String line) throws Exception{
//                String[] arr=line.split("/t");
//                Integer id=Integer.parseInt(arr[0]);
//                String name=arr[1];
//                String male=arr[2];
//                String grade=arr[3];
//                return RowFactory.create(id,name,male,grade);
//            }
//        });
//        StructField[] s_fields=new StructField[3];
//        s_fields[0]=new StructField("id", DataTypes.IntegerType,false, Metadata.empty());
//        s_fields[1]=new StructField("name",DataTypes.StringType,true,Metadata.empty());
//        s_fields[2]=new StructField("male",DataTypes.StringType,true,Metadata.empty());
//        s_fields[3]=new StructField("grade",DataTypes.StringType,true,Metadata.empty());
//        StructType s_type=new StructType(s_fields);
//        Dataset<Row> s_df1=spark.createDataFrame(s_rdd2,s_type);
//        //注册临时视图
//        s_df1.createOrReplaceTempView("student_v");
//
//        //scores
//        JavaRDD<String> o_rdd1=javaSparkContext.textFile("/user/hadoop/data2/orders.txt");
//        JavaRDD<Row> o_rdd2=o_rdd1.map(new Function<String, Row>() {
//            public Row call(String line) throws Exception{
//                String[] arr=line.split(",");
//                Integer id=Integer.parseInt(arr[0]);
//                Integer studentId=Integer.parseInt(arr[1]);
//                String subject=arr[2];
//                double score=Double.parseDouble(arr[3]);
//                return RowFactory.create(id,studentId,subject,score);
//            }
//        });
//        StructField[] o_fields=new StructField[4];
//        o_fields[0]=new StructField("id", DataTypes.IntegerType,false, Metadata.empty());
//        o_fields[1]=new StructField("student_id",DataTypes.IntegerType,true,Metadata.empty());
//        o_fields[2]=new StructField("subject",DataTypes.StringType,true,Metadata.empty());
//        o_fields[3]=new StructField("score", DataTypes.DoubleType,false, Metadata.empty());
//        StructType o_type=new StructType(o_fields);
//        Dataset<Row> o_df1=spark.createDataFrame(o_rdd2,o_type);
//        //注册临时视图
//        o_df1.createOrReplaceTempView("score_v");
//
//        spark.sql("select * from student_v").show();
//        spark.sql("select * from score_v").show();
//        String sql="select c.id,c.name,ifnull(o._sum,0) total_price from student_v c left outer join (select cid,sum(price) _sum from _order group by cid) o on c.id=o.cid";
//        spark.sql(sql).show(1000,false);
//
//    }
//}
