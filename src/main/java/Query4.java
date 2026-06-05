import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;

public class Query4 {
    public static void run(SparkSession spark, Dataset<Row> df) {
    
        long t0 = System.currentTimeMillis();

        //Clean the data (replace NULL by 0)
        Dataset<Row> cleanDf = df.na().fill(0.0, new String[]{
            "CANCELLED", "DIVERTED", "ARR_DELAY", "DEP_DELAY",
            "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY",
            "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"
        });

        // Top 15 
        Dataset<Row> flightCount = cleanDf
            .groupBy("OP_UNIQUE_CARRIER")
            .agg(count("*").alias("number_of_flights"))
            .orderBy(col("number_of_flights").desc())
            .limit(15);

        Dataset<Row> flights = cleanDf.join(flightCount, "OP_UNIQUE_CARRIER");
        flights.cache();

        // Cancellation rate 
        Dataset<Row> cancRate = flights
            .groupBy("OP_UNIQUE_CARRIER")
            .agg(
                sum(col("CANCELLED")).divide(count("*"))
                    .multiply(100).alias("cancellation_rate")
            );

        // Valid flight and delay avg cvause 
        Dataset<Row> validFlights = flights
            .filter(col("CANCELLED").equalTo(0)
                .and(col("DIVERTED").equalTo(0)));

        Dataset<Row> delayStats = validFlights
            .groupBy("OP_UNIQUE_CARRIER")
            .agg(
                avg("ARR_DELAY").alias("avg_arr_delay"),
                avg("DEP_DELAY").alias("avg_dep_delay"),
                avg("CARRIER_DELAY").alias("avg_carrier_delay"),
                avg("WEATHER_DELAY").alias("avg_weather_delay"),
                avg("NAS_DELAY").alias("avg_nas_delay"),
                avg("SECURITY_DELAY").alias("avg_security_delay"),
                avg("LATE_AIRCRAFT_DELAY").alias("avg_late_aircraft_delay")
            );


        Dataset<Row> stats = delayStats.join(cancRate, "OP_UNIQUE_CARRIER");
        
        //Muster the data on VectorAssemblor
        VectorAssembler assembler = new VectorAssembler()
            .setInputCols(new String[]{
                "avg_arr_delay",
                "avg_dep_delay",
                "cancellation_rate",
                "avg_carrier_delay",
                "avg_weather_delay",
                "avg_nas_delay",
                "avg_security_delay",
                "avg_late_aircraft_delay"
            })
            .setOutputCol("features")
            .setHandleInvalid("skip");

        Dataset<Row> assembled = assembler.transform(stats);

        //Normalize the data
        StandardScaler scaler = new StandardScaler()
            .setInputCol("features")
            .setOutputCol("scaled_features")
            .setWithMean(true)
            .setWithStd(true);
        
        StandardScalerModel scalerModel = scaler.fit(assembled);
        Dataset<Row> scaledData = scalerModel.transform(assembled);


        // K-Means
        KMeans kmeans = new KMeans()
            .setK(2)
            .setFeaturesCol("scaled_features")
            .setPredictionCol("cluster")
            .setSeed(42);

        KMeansModel model = kmeans.fit(scaledData);
        Dataset<Row> predictions = model.transform(scaledData);

        long t1 = System.currentTimeMillis();

        // Save the data
        predictions.select(
            col("OP_UNIQUE_CARRIER"),
            col("avg_arr_delay"),
            col("avg_dep_delay"),
            col("cancellation_rate"),
            col("avg_carrier_delay"),
            col("avg_weather_delay"),
            col("avg_nas_delay"),
            col("avg_security_delay"),
            col("avg_late_aircraft_delay"),
            col("cluster")
        )
        .coalesce(1)
        .write()
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .csv("hdfs://namenode:9000/results/query4");
        
        predictions.select(
            col("OP_UNIQUE_CARRIER"),
            col("avg_arr_delay"),
            col("avg_dep_delay"),
            col("cancellation_rate"),
            col("avg_carrier_delay"),
            col("avg_weather_delay"),
            col("avg_nas_delay"),
            col("avg_security_delay"),
            col("avg_late_aircraft_delay"),
            col("cluster")
        )
        .coalesce(1)
        .write()
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .csv("/spark/results/query4");

        long t2 = System.currentTimeMillis();
        System.out.println("\n" +
            "╔══════════════════════════════════════╗\n" +
            "║         [Q4] EXECUTION TIMES         ║\n" +
            "╠══════════════════════════════════════╣\n" +
            "║  Computation    : " + String.format("%-18s", (t1-t0)+" ms") + "║\n" +
            "║  Output         : " + String.format("%-18s", (t2-t1)+" ms") + "║\n" +
            "║  TOTAL          : " + String.format("%-18s", (t2-t0)+" ms") + "║\n" +
            "╚══════════════════════════════════════╝");

        flights.unpersist();
    }
}
