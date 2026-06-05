import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

//Program to see if the architecture works
public class TestSpark {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
            .appName("TestSpark")
            .getOrCreate();

        System.out.println("=== Spark version : " + spark.version());
        System.out.println("=== Test chargement CSV...");

        Dataset<Row> df = spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("spark/data/*.csv");

        System.out.println("=== Nombre de lignes : " + df.count());
        System.out.println("=== Colonnes disponibles :");
        for (String col : df.columns()) {
            System.out.println("    - " + col);
        }

        System.out.println("=== Aperçu des 5 premières lignes :");
        df.show(5);
        Dataset<Row> filtered = df.filter(col("OP_UNIQUE_CARRIER").equalTo("F9"));
        Dataset<Row> f9CancelDiverted = filtered.filter(col("CANCELLED").equalTo("0").and(col("DIVERTED").equalTo("0")));
        Dataset<Row> stats = f9CancelDiverted.agg(
        count("SECURITY_DELAY").alias("total_flight"),
        avg("SECURITY_DELAY").alias("avg_security_delay")
        );
        System.out.println("=== Resultat F9 :");
        stats.show();   
        spark.stop();
    }
}