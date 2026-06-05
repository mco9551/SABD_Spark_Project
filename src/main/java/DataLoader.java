import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataLoader {
    public static Dataset<Row> load(SparkSession spark) {
    return spark.read()
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("hdfs://namenode:9000/data/*.csv"); 
}
}