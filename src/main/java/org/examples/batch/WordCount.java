package org.examples.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;

public class WordCount {

    private final SparkSession spark;

    WordCount() {
        this.spark = createSessionObj();
    }

    private SparkSession createSessionObj() {
        SparkConf conf = new SparkConf();
        return SparkSession
                .builder()
                .config(conf)
                .master("local[*]")
                .appName("word count")
                .getOrCreate();
    }

    public static void main(String[] args) {
        WordCount obj = new WordCount();
        String inputPath = "src/main/resources/Lorem-Ipsum.txt";
        String outputPath = "src/main/resources/word-count-output/";
        SparkContext sparkContext = obj.spark.sparkContext();

        try(JavaSparkContext jsc = new JavaSparkContext(sparkContext)) {
            // Read line by line
            JavaRDD<String> lines = jsc.textFile(inputPath);

            // Split the line in to words
            JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());

            // Create word count, happens in each executor
            JavaPairRDD<String, Integer> wordPair = words.mapToPair(word -> new Tuple2<>(word, 1));

            // Group each keys and sum its count, shuffle same keys to same executor
            JavaPairRDD<String, Integer> wordCounts = wordPair.reduceByKey((a,b) -> a + b);

            // Delete output folder if already present
            File outputFolder = new File(outputPath);
            if (outputFolder.exists()) {
                obj.deleteDirectory(outputFolder);
            }

            // Save as a text file
            wordCounts.coalesce(1).saveAsTextFile(outputPath);
        }
    }

    public boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

}