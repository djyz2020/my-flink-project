package myspark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Created by haibozhang on 2019/8/3.
 */
public class SimpleApp {

    public static void main(String[] args) {

        // 示例一
        SparkSession spark = SparkSession.builder().appName("sparkDemo").master("local[*]").getOrCreate();
        Dataset<String> data = spark.read().textFile("src\\main\\resources\\UserBehavior.csv").cache();
        long size = data.collectAsList().stream().count();
        System.out.println("总记录条数：[" + size + "]");
        spark.stop();

        // 示例二
//        SparkConf conf = new SparkConf().setAppName("sparkDemo").setMaster("local[*]");
//        SparkContext sc = new SparkContext(conf).getOrCreate();
//        RDD<String> data = sc.textFile("src\\main\\resources\\UserBehavior.csv", 10);
//        System.out.println(data.count());

    }

}
