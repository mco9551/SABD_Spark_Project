import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.percentile_approx;
import org.apache.spark.sql.types.DataTypes;

public class Query3 {
    public static void run(SparkSession spark, Dataset<Row> df) {
    
        long t0 = System.currentTimeMillis();

        //Filter the company 
        Dataset<Row> validFlights = df
        .filter(col("OP_UNIQUE_CARRIER").equalTo("AA")
            .or(col("OP_UNIQUE_CARRIER").equalTo("DL"))
            .or(col("OP_UNIQUE_CARRIER").equalTo("UA"))
            .or(col("OP_UNIQUE_CARRIER").equalTo("WN"))
        );
        validFlights.cache();

        //aggregate data 
        Dataset<Row> withHour = validFlights
            .withColumn("hour", col("CRS_DEP_TIME").divide(100).cast(DataTypes.IntegerType));

        //Percentile
        Dataset<Row> results =  withHour
            .groupBy("OP_UNIQUE_CARRIER", "hour")
            .agg(
                count("*").alias("total_flight"),                           
                percentile_approx(col("DEP_DELAY"), lit(0.25), lit(1000)).alias("p25"),
                percentile_approx(col("DEP_DELAY"), lit(0.50), lit(1000)).alias("p50"),
                percentile_approx(col("DEP_DELAY"), lit(0.75), lit(1000)).alias("p75"),
                percentile_approx(col("DEP_DELAY"), lit(0.90), lit(1000)).alias("p90")

            ).orderBy("OP_UNIQUE_CARRIER", "hour");

        Dataset<Row> delay = validFlights
        .filter(col("CANCELLED").equalTo(0))
        .groupBy("OP_UNIQUE_CARRIER")
        .agg(
            min("DEP_DELAY").alias("min_delay"),
            max("DEP_DELAY").alias("max_delay")
        );

        long t1 = System.currentTimeMillis();

        results.show();
        delay.show();

        //Save in two folders 
        results.coalesce(1)
            .write()
            .option("header", "true")
            .mode(SaveMode.Overwrite)
            .csv("hdfs://namenode:9000/results/query3/percentile");

        delay.coalesce(1)
             .write()
            .option("header", true)
            .mode(SaveMode.Overwrite)
            .csv("hdfs://namenode:9000/results/query3/delay");
            
            results.coalesce(1)
            .write()
            .option("header", "true")
            .mode(SaveMode.Overwrite)
            .csv("/spark/results/query3/percentile");

        delay.coalesce(1)
             .write()
            .option("header", true)
            .mode(SaveMode.Overwrite)
            .csv("/spark/results/query3/delay");

        long t2 = System.currentTimeMillis();
        System.out.println("\n" +
            "╔══════════════════════════════════════╗\n" +
            "║         [Q3] EXECUTION TIMES         ║\n" +
            "╠══════════════════════════════════════╣\n" +
            "║  Computation    : " + String.format("%-18s", (t1-t0)+" ms") + "║\n" +
            "║  Output         : " + String.format("%-18s", (t2-t1)+" ms") + "║\n" +
            "║  TOTAL          : " + String.format("%-18s", (t2-t0)+" ms") + "║\n" +
            "╚══════════════════════════════════════╝");
    }
}
