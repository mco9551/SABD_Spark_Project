import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
            .appName("SABD-Project1")
            .getOrCreate();
        
        spark.sparkContext().setLogLevel("WARN");
      
        String query = args.length > 0 ? args[0] : "0"; //By default (no arguments given on the console) we run the Query 1
        int iterations = args.length > 1 ? Integer.parseInt(args[1]) : 3; //Number of iteration to compute the average time. By default 3

        long t0 = System.currentTimeMillis();
        Dataset<Row> flights = DataLoader.load(spark); //Load the data of the flight 
        flights.cache(); //Store the DATA on the ram
        System.out.println("=== Loading : " + (System.currentTimeMillis() - t0) + " ms");

        long[] times = new long[iterations];
        for (int i = 0; i < iterations; i++) {
            System.out.println("=== Iteration " + (i+1) + "/" + iterations);
            long start = System.currentTimeMillis();
            switch (query) {
                case "0": 
                System.out.println("Execution of all the query");
                Query1.run(spark, flights);
                Query2.run(spark, flights);
                Query3.run(spark, flights);
                Query4.run(spark, flights);
                break;
                case "1": Query1.run(spark, flights); break;
                case "2": Query2.run(spark, flights); break;
                case "3": Query3.run(spark, flights); break;
                case "4": Query4.run(spark, flights); break;
                default : System.out.println("The Query doesn't exist try again");
            }
            times[i] = System.currentTimeMillis() - start;
        }

        long sum = 0;
        for (long t : times) sum += t;
        System.out.println("=== Average time : " + (sum / iterations) + " ms");

        spark.stop();
    }
}
