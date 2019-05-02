import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class KafkaConsumer {
    public static void main(String[] args) throws StreamingQueryException {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("DeduplicationPOC").getOrCreate();

        sparkSession.sparkContext().setLogLevel("ERROR");

        Dataset<Row> csvDF = sparkSession.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "CSVTopic")
                .load();

        Dataset<Row> keyValueDataset = csvDF.selectExpr("CAST(value AS STRING)");

        Dataset<Row> keyValueDatasetUnique = keyValueDataset.dropDuplicates();

        keyValueDatasetUnique.writeStream()
                .format("console")
                .start()
                .awaitTermination();
    }
}