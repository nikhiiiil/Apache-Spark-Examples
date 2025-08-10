package org.examples.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.Arrays;

public class UniqueWords {

    private final SparkSession spark;

    public UniqueWords() {
        this.spark = SparkSession
                .builder()
                .config(new SparkConf())
                .appName("UniqueWords")
                .master("local[*]")
                .getOrCreate();
    }

    public static void main(String[] args) {
        UniqueWords obj = new UniqueWords();
        String inputPath = "src/main/resources/Lorem-Ipsum.txt";
        String outputPath = "src/main/resources/output-unique-word/";

        SparkContext context = obj.spark.sparkContext();
        try(JavaSparkContext jsc = new JavaSparkContext(context)) {

            JavaRDD<String> lines = jsc.textFile(inputPath);
            JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());
            JavaRDD<String> uniqueWords = words.distinct();

            // Delete output folder if already present
            File outputFolder = new File(outputPath);
            if (outputFolder.exists()) {
                System.out.println("Deleting folder : " + obj.deleteDirectory(outputFolder));
            }

            uniqueWords.coalesce(1).saveAsTextFile(outputPath);
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
