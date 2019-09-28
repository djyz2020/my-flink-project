package myspark;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Created by haibozhang on 2019/8/3.
 */
public class SimpleSparkDemo {

    public static void main(String[] args) {

        // 示例一
//        SparkSession spark = SparkSession.builder().appName("sparkDemo").master("local[*]").getOrCreate();
//        Dataset<String> data = spark.read().textFile("src\\main\\resources\\UserBehavior.csv").cache();
//        long size = data.collectAsList().stream().count();
//        System.out.println("总记录条数：[" + size + "]");
//        spark.stop();

        // 示例二
//        SparkConf conf = new SparkConf().setAppName("sparkDemo").setMaster("local[*]");
//        SparkContext sc = new SparkContext(conf).getOrCreate();
//        RDD<String> data = sc.textFile("src\\main\\resources\\UserBehavior.csv", 10);
//        System.out.println(data.count());

        // 示例三
        SparkSession spark = SparkSession.builder()
                .appName("sparkSql")
                .master("local[*]")
                .getOrCreate();

        String schemaString = "name age";
        List<StructField> fields = new ArrayList<>();
        for(String filedName : schemaString.split(" ")){
            StructField field = DataTypes.createStructField(filedName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> df = spark.read().schema(schema).json("src\\main\\resources\\data.json")
                .filter(new FilterFunction<Row>() {
                    @Override
                    public boolean call(Row value) throws Exception {
                        if (value.get(0) == null) {
                            return false;
                        }
                        return true;
                    }
                });
        df.show();
        System.out.println("-----------------------------------------------------");
        df.printSchema();
        System.out.println("-----------------------------------------------------");
        df.select("name").show();
        System.out.println("-----------------------------------------------------");
        df.select(col("age")).show();

    }

}
