package org.examples.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class StreamingWordCount {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        StreamingWordCount obj = new StreamingWordCount();

        SparkSession spark = SparkSession
                .builder()
                .appName("Streaming Word Count")
                .master("local[*]")
                .config(new SparkConf())
                .getOrCreate();

        Dataset<Row> input = spark.readStream()
             .format("socket")
             .option("host", "localhost")
             .option("port", "9999")
             .load();

        Dataset<Row> words = obj.countWords(input);

        words.writeStream()
             .outputMode("complete")
             .format("console")
             .trigger(Trigger.ProcessingTime("5 seconds"))
             .start()
             .awaitTermination();
    }

    public Dataset<Row> countWords(Dataset<Row> ds) {
        Dataset<Row> wordDF = ds.select(explode(split(col("value"), "\\s+")).alias("word"));
        return wordDF.groupBy(col("word")).count();
    }
}
