package com.sparksql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

import static javax.ws.rs.client.Entity.json;

/**
 * spark直接读取文件，写入HDFS
 *
 * 注意：文件URL
 * file:///home/myspark/libraryJson.txt
 * hdfs://192.168.1.110:9000/data/sparksql/student.json
 */
public class SparkSqlDemo {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder().appName("sparkSQL")
                .master("local")
                .getOrCreate();
        JavaSparkContext javaSparkContext=new JavaSparkContext(spark.sparkContext());
        /**
         * 注意：Json 中文编码、日期格式等
         * 例如：
         * {"id": 1, "name": "格林童话", "classify": "童书", "price": 98.5, "date": "2020-02-03", "lib_id": 1}
         * {"id": 2, "name": "JAVA编程", "classify": "教育", "price": 203, "date": "1998-04-03", "lib_id": 2}
         *
         * book_tbl
         * id,name,classify,price,date,lib_id
         *
         * library_tbl
         * id,name
         */
        StructField[] o_fields=new StructField[6];
        o_fields[0]=new StructField("id", DataTypes.IntegerType,false, Metadata.empty());
        o_fields[1]=new StructField("name",DataTypes.StringType,true,Metadata.empty());
        o_fields[2]=new StructField("classify",DataTypes.StringType,true,Metadata.empty());
        o_fields[3]=new StructField("price",DataTypes.createDecimalType(),true,Metadata.empty());
        o_fields[4]=new StructField("date",DataTypes.DateType,true,Metadata.empty());
        o_fields[5]=new StructField("lib_id",DataTypes.IntegerType,false,Metadata.empty());

        StructType o_type=new StructType(o_fields);
        Dataset<Row> bds=spark.read().schema(o_type).json("file:///home/myspark/bookJson.txt");
        bds.show();
        //创建临时视图
        bds.createOrReplaceTempView("book_v");
        bds=spark.sql("select * from book_v");
        System.out.println("=============select * from book_v:============");
        bds.show();

        StructField[] l_fields=new StructField[2];
        l_fields[0]=new StructField("id", DataTypes.IntegerType,false, Metadata.empty());
        l_fields[1]=new StructField("name",DataTypes.StringType,true,Metadata.empty());
        StructType l_type=new StructType(l_fields);
        Dataset<Row> lds=spark.read().schema(l_type).json("file:///home/myspark/libraryJson.txt");
        lds.show();
//        创建临时视图
        lds.createOrReplaceTempView("library_v");
        lds=spark.sql("select * from library_v");
        System.out.println("=============select * from library_v:============");
        lds.show();

        System.out.println("select classify,sum(price),count(1) from book_v group by classify：");
        Dataset<Row> ds1 = spark.sql("select classify,sum(price),count(1) from book_v group by classify");
        ds1.show();

        System.out.println("select t.id,t.name,count(1) from (select a.price,b.id,b.name from book_v a, library_v b where a.lib_id=b.id) t group by t.id,t.name");
        Dataset<Row> ds2 = spark.sql("select t.id,t.name,count(1) from (select a.price,b.id,b.name from book_v a, library_v b where a.lib_id=b.id) t group by t.id,t.name");
        ds2.show();


        //保存处理
//        ds1.write().json("/home/myspark/bookSum.txt");
        //保存处理,设置保存模式（以追加的方式保存）
        // df2.write().mode(SaveMode.Append).json("file:///J:\\Program\\file\\comp\\out\\outjon.dat\\1.json");



    }
}
