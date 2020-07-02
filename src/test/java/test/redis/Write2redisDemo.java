package test.redis;




import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.SaveMode.*;
import com.redislabs.provider.redis.*;

/**
 *
 * https://github.com/RedisLabs/spark-redis/blob/master/doc/dataframe.md
 */

public class Write2redisDemo {

    private static final StructType SCHEMA = FIELD_SCHEMA();
    private static final Encoder<Row> ENC = RowEncoder.apply(SCHEMA);

    private static StructType FIELD_SCHEMA() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("key_name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true)); //TODO ervery field ,every tyep; should get map. foreach it
        fields.add(DataTypes.createStructField("age", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("salary", DataTypes.StringType, true));
        return new StructType(fields.toArray(new StructField[fields.size()]));
    }

    public static void main(String[] args) {

        final SparkConf sparkConf = new SparkConf()
                .setAppName("writeRedisTest")
                .setMaster("local[*]");


        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                //  TODO your host
                .config("spark.redis.host", "your host")
                // TODO your port
                .config("spark.redis.port", "6379")
                // TODO your password
                .config("spark.redis.auth", "password")
                .getOrCreate();

//        SparkSession spark = SparkSession
//                .builder()
//                .appName("MyApp")
//                .master("local[*]")
//                .config("spark.redis.host", "10.19.180.60")
//                .config("spark.redis.port", "6379")
//                .config("spark.redis.auth", "Redisredis")
//                .getOrCreate();

        List<Row> list = new ArrayList<Row>();
        list.add(RowFactory.create("Tom", "Tom","22","5000"));
        list.add(RowFactory.create("Sally", "Sally","20","6000"));

        Dataset<Row> dataset = spark.createDataset(list, ENC);



//        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
//                new Person("John", 35),
//                new Person("Peter", 40)), Person.class);

//        df.write().format("org.apache.spark.sql.redis")
//                .option("table", "person")
//                .option("key.column", "name")
//                .mode(SaveMode.Append)
//                .save();



        Map<String, String> optionMap = Maps.of();
        optionMap.put("table","writeRedis");
        optionMap.put("key.column", "key_name");


        dataset.write().format("org.apache.spark.sql.redis")
                .options(optionMap)
                .mode(SaveMode.Append)
                .save();
    }

}
