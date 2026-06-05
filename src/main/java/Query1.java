import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.sum;

public class Query1 {
    public static void run(SparkSession spark, Dataset<Row> flights) {
        long t0 = System.currentTimeMillis();

        // Filter AA et DL
        Dataset<Row> filtered = flights
            .filter(col("OP_UNIQUE_CARRIER").equalTo("AA")
                .or(col("OP_UNIQUE_CARRIER").equalTo("DL")));

        filtered.cache();

        // Filter Non cancelled flights
        Dataset<Row> nonCancelled = filtered
            .filter(col("CANCELLED").isNotNull()
                .and(col("CANCELLED").equalTo(0.0)));

        // Stats delays
        Dataset<Row> delayStats = nonCancelled
            .groupBy("MONTH", "OP_UNIQUE_CARRIER")
            .agg(
                avg("DEP_DELAY").alias("dep_delay_mean"),
                min("DEP_DELAY").alias("dep_delay_min"),
                max("DEP_DELAY").alias("dep_delay_max")
            );

        // cancellation rate 
        Dataset<Row> cancRate = filtered
            .groupBy("MONTH", "OP_UNIQUE_CARRIER")
            .agg(
                sum(col("CANCELLED")).divide(count("*")).multiply(100)
                    .alias("cancellation_rate")
            );

        // Join
        Dataset<Row> result = delayStats
            .join(cancRate,
                delayStats.col("MONTH").equalTo(cancRate.col("MONTH"))
                .and(delayStats.col("OP_UNIQUE_CARRIER")
                    .equalTo(cancRate.col("OP_UNIQUE_CARRIER"))),
                "inner")
            .select(
                delayStats.col("MONTH"),
                delayStats.col("OP_UNIQUE_CARRIER"),
                col("dep_delay_mean"),
                col("dep_delay_min"),
                col("dep_delay_max"),
                col("cancellation_rate")
            )
            .orderBy("OP_UNIQUE_CARRIER", "MONTH");

        long t1 = System.currentTimeMillis();

        result.show();
        result.coalesce(1)
            .write()
            .option("header", "true")
            .mode(SaveMode.Overwrite)
            .csv("hdfs://namenode:9000/results/query1"); //save on HDFS
            
            result.coalesce(1)
            .write()
            .option("header", "true")
            .mode(SaveMode.Overwrite)
            .csv("/spark/results/query1");

        long t2 = System.currentTimeMillis();
        System.out.println("\n" +
            "╔══════════════════════════════════════╗\n" +
            "║         [Q1] EXECUTION TIMES         ║\n" +
            "╠══════════════════════════════════════╣\n" +
            "║  Computation    : " + String.format("%-18s", (t1-t0)+" ms") + "║\n" +
            "║  Output         : " + String.format("%-18s", (t2-t1)+" ms") + "║\n" +
            "║  TOTAL          : " + String.format("%-18s", (t2-t0)+" ms") + "║\n" +
            "╚══════════════════════════════════════╝");

        filtered.unpersist();
    }
}
