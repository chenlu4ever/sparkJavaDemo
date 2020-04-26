//package scala;
//
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.KeyValue;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.Admin;
//import org.apache.hadoop.hbase.client.Connection;
//import org.apache.hadoop.hbase.client.ConnectionFactory;
//import org.apache.hadoop.hbase.client.Table;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
//import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.PairFlatMapFunction;
//import org.apache.spark.storage.StorageLevel;
//
//import util.HFileLoader;
//
//public class SparkHbaseDemo {
//
//    private static final String ZKconnect="slave1,slave2,slave3:2181";
//    private static final String HDFS_ADDR="hdfs://master:8020";
//    private static final String TABLE_NAME="DBSTK.STKFSTEST";//表名
//    private static final String COLUMN_FAMILY="FS";//列族
//
//    public static void run(String[] args) throws Exception {
//        Configuration configuration = HBaseConfiguration.create();
//        configuration.set("hbase.zookeeper.quorum", ZKconnect);
//        configuration.set("fs.defaultFS", HDFS_ADDR);
//        configuration.set("dfs.replication", "1");
//
//        String inputPath = args[0];
//        String outputPath = args[1];
//        Job job = Job.getInstance(configuration, "Spark Bulk Loading HBase Table:" + TABLE_NAME);
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setMapOutputKeyClass(ImmutableBytesWritable.class);//指定输出键类
//        job.setMapOutputValueClass(KeyValue.class);//指定输出值类
//        job.setOutputFormatClass(HFileOutputFormat2.class);
//
//        FileInputFormat.addInputPaths(job, inputPath);//输入路径
//        FileSystem fs = FileSystem.get(configuration);
//        Path output = new Path(outputPath);
//        if (fs.exists(output)) {
//            fs.delete(output, true);//如果输出路径存在，就将其删除
//        }
//        fs.close();
//        FileOutputFormat.setOutputPath(job, output);//hfile输出路径
//
//        //初始化sparkContext
//        SparkConf sparkConf = new SparkConf().setAppName("HbaseBulkLoad").setMaster("local[*]");
//        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//        //读取数据文件
//        JavaRDD<String> lines = jsc.textFile(inputPath);
//        lines.persist(StorageLevel.MEMORY_AND_DISK_SER());
//        JavaPairRDD<ImmutableBytesWritable,KeyValue> hfileRdd =
//                lines.flatMapToPair(new PairFlatMapFunction<String, ImmutableBytesWritable, KeyValue>() {
//                    private static final long serialVersionUID = 1L;
//                    @Override
//                    public Iterator<Tuple2<ImmutableBytesWritable, KeyValue>> call(String text) throws Exception {
//                        List<Tuple2<ImmutableBytesWritable, KeyValue>> tps = new ArrayList<Tuple2<ImmutableBytesWritable, KeyValue>>();
//                        if(null == text || text.length()<1){
//                            return tps.iterator();//不能返回null
//                        }
//                        String[] resArr = text.split(",");
//                        if(resArr != null && resArr.length == 14){
//                            byte[] rowkeyByte = Bytes.toBytes(resArr[0]+resArr[3]+resArr[4]+resArr[5])
//                            byte[] columnFamily = Bytes.toBytes(COLUMN_FAMILY);
//                            ImmutableBytesWritable ibw = new ImmutableBytesWritable(rowkeyByte);
//                            //EP,HP,LP,MK,MT,SC,SN,SP,ST,SY,TD,TM,TQ,UX（字典顺序排序）
//                            //注意，这地方rowkey、列族和列都要按照字典排序，如果有多个列族，也要按照字典排序，rowkey排序我们交给spark的sortByKey去管理
//                            tps.add(new Tuple2<>(ibw,new KeyValue(rowkeyByte, columnFamily, Bytes.toBytes("EP"),Bytes.toBytes(resArr[9]))));
//                            tps.add(new Tuple2<>(ibw,new KeyValue(rowkeyByte, columnFamily, Bytes.toBytes("HP"),Bytes.toBytes(resArr[7]))));
//                            tps.add(new Tuple2<>(ibw,new KeyValue(rowkeyByte, columnFamily, Bytes.toBytes("LP"),Bytes.toBytes(resArr[8]))));
//                            tps.add(new Tuple2<>(ibw,new KeyValue(rowkeyByte, columnFamily, Bytes.toBytes("MK"),Bytes.toBytes(resArr[13]))));
//                            tps.add(new Tuple2<>(ibw,new KeyValue(rowkeyByte, columnFamily, Bytes.toBytes("MT"),Bytes.toBytes(resArr[4]))));
//                            tps.add(new Tuple2<>(ibw,new KeyValue(rowkeyByte, columnFamily, Bytes.toBytes("SC"),Bytes.toBytes(resArr[0]))));
//                            tps.add(new Tuple2<>(ibw,new KeyValue(rowkeyByte, columnFamily, Bytes.toBytes("SN"),Bytes.toBytes(resArr[1]))));
//                            tps.add(new Tuple2<>(ibw,new KeyValue(rowkeyByte, columnFamily, Bytes.toBytes("SP"),Bytes.toBytes(resArr[6]))));
//                            tps.add(new Tuple2<>(ibw,new KeyValue(rowkeyByte, columnFamily, Bytes.toBytes("ST"),Bytes.toBytes(resArr[5]))));
//                            tps.add(new Tuple2<>(ibw,new KeyValue(rowkeyByte, columnFamily, Bytes.toBytes("SY"),Bytes.toBytes(resArr[2]))));
//                            tps.add(new Tuple2<>(ibw,new KeyValue(rowkeyByte, columnFamily, Bytes.toBytes("TD"),Bytes.toBytes(resArr[3]))));
//                            tps.add(new Tuple2<>(ibw,new KeyValue(rowkeyByte, columnFamily, Bytes.toBytes("TM"),Bytes.toBytes(resArr[11]))));
//                            tps.add(new Tuple2<>(ibw,new KeyValue(rowkeyByte, columnFamily, Bytes.toBytes("TQ"),Bytes.toBytes(resArr[10]))));
//                            tps.add(new Tuple2<>(ibw,new KeyValue(rowkeyByte, columnFamily, Bytes.toBytes("UX"),Bytes.toBytes(resArr[12]))));
//                        }
//                        return tps.iterator();
//                    }
//                }).sortByKey();
//
//        Connection connection = ConnectionFactory.createConnection(configuration);
//        TableName tableName = TableName.valueOf(TABLE_NAME);
//        HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(tableName), connection.getRegionLocator(tableName));
//
//        //生成hfile文件
//        hfileRdd.saveAsNewAPIHadoopFile(outputPath, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, job.getConfiguration());
//
//        // bulk load start
//        Table table = connection.getTable(tableName);
//        Admin admin = connection.getAdmin();
//        LoadIncrementalHFiles load = new LoadIncrementalHFiles(configuration);
//        load.doBulkLoad(new Path(outputPath), admin,table,connection.getRegionLocator(tableName));
//
//        jsc.close();
//    }
//
//    public static void main(String[] args) {
//        try {
//            long start = System.currentTimeMillis();
//            args = new String[]{"hdfs://master:8020/test/test.txt","hdfs://master:8020/test/hfile/test"};
//            run(args);
//            long end = System.currentTimeMillis();
//            System.out.println("数据导入成功，总计耗时："+(end-start)/1000+"s");
//        } catch(Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//}