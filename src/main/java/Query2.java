import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class Query2 {
    public static void run(SparkSession spark, Dataset<Row> df) {
    
        long t0 = System.currentTimeMillis();

        // FIlter non cancelled and non diverted
        Dataset<Row> validFlights = df.filter(
            col("CANCELLED").equalTo(0).and(col("DIVERTED").equalTo(0))
        );

        //stats
        Dataset<Row> stats = validFlights
            .groupBy("OP_UNIQUE_CARRIER")
            .agg(
                count("*").alias("total_flights"),
                avg("ARR_DELAY").alias("avg_arr_delay"),
                avg("DEP_DELAY").alias("avg_dep_delay"),
                avg("CARRIER_DELAY").alias("avg_carrier_delay"),
                avg("WEATHER_DELAY").alias("avg_weather_delay"),
                avg("NAS_DELAY").alias("avg_nas_delay"),
                avg("SECURITY_DELAY").alias("avg_security_delay"),
                avg("LATE_AIRCRAFT_DELAY").alias("avg_late_aircraft_delay")
            );


        // Filter more than 500 flights, keep the top 10
        Dataset<Row> result = stats
            .filter(col("total_flights").geq(500))
            .orderBy(col("avg_arr_delay").desc())
            .limit(10);

        long t1 = System.currentTimeMillis();

        result.show();

        // Save the data
        result.coalesce(1)
            .write()
            .option("header", "true")
            .mode(SaveMode.Overwrite)
            .csv("hdfs://namenode:9000/results/query2");
            
            result.coalesce(1)
            .write()
            .option("header", "true")
            .mode(SaveMode.Overwrite)
            .csv("/spark/results/query2");

        long t2 = System.currentTimeMillis();
        System.out.println("\n" +
            "╔══════════════════════════════════════╗\n" +
            "║         [Q2] EXECUTION TIMES         ║\n" +
            "╠══════════════════════════════════════╣\n" +
            "║  Computation    : " + String.format("%-18s", (t1-t0)+" ms") + "║\n" +
            "║  Output         : " + String.format("%-18s", (t2-t1)+" ms") + "║\n" +
            "║  TOTAL          : " + String.format("%-18s", (t2-t0)+" ms") + "║\n" +
            "╚══════════════════════════════════════╝");
    }
}
