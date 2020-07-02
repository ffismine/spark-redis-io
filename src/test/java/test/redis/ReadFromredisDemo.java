package test.redis;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;


/**
 *
 * https://github.com/RedisLabs/spark-redis/blob/master/doc/dataframe.md
 */



public class ReadFromredisDemo {
    static final StructType SCHEMA = FIELD_SCHEMA();

    private static StructType FIELD_SCHEMA() {
        List<StructField> fields = new ArrayList<>();
        //       fields.add(DataTypes.createStructField("key_name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("salary", DataTypes.StringType, true));
        return new StructType(fields.toArray(new StructField[fields.size()]));
    }

    public static void main(String[] args) {

        final SparkConf sparkConf = new SparkConf()
                .setAppName("readRedisTest")
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

        Dataset<Row> dataset = spark.read()
                .format("org.apache.spark.sql.redis")
                .schema(SCHEMA)
                .option("keys.pattern", "writeRedis:*")
                .option("key.column", "name")
                .load();
        dataset.printSchema();
        dataset.show();
    }

}
