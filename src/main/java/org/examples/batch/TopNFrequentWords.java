package org.examples.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TopNFrequentWords {

    private final SparkSession spark;

    public TopNFrequentWords() {
        this.spark = SparkSession
                .builder()
                .config(new SparkConf())
                .master("local[1]")
                .appName("Top N Frequency Words")
                .getOrCreate();
    }

    public static void main(String[] args) {
        int n = 2;
        TopNFrequentWords obj = new TopNFrequentWords();
        String inputPath = "src/main/resources/Lorem-Ipsum.txt";

        try (JavaSparkContext jsc = new JavaSparkContext(obj.spark.sparkContext())) {
            JavaPairRDD<String, Integer> wordCount = jsc.textFile(inputPath)
                    .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                    .mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey(Integer::sum);

            JavaPairRDD<Integer, String> numberWord = wordCount
                    .mapToPair(pair -> new Tuple2<>(pair._2, pair._1));

            List<Tuple2<Integer, String>> nWords = numberWord
                    .sortByKey(false)
                    .take(n);

            nWords.forEach(x -> System.out.println(x._2));
        }
    }
}