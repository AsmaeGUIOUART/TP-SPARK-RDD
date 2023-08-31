package ma.enset.tp1sparkrdd;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Application2 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("RDD WORD COUNT").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> rddLines=sc.textFile("hdfs://localhost:9000/words.txt");
        JavaRDD<String> rddNames=rddLines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String,Integer> rddPairs=rddNames.mapToPair(s -> new Tuple2<>(s,1));
        JavaPairRDD<String,Integer> wordCount=rddPairs.reduceByKey((a, b) -> a+b);

        List<Tuple2<String,Integer>> elems=wordCount.collect();
        for (Tuple2<String,Integer> t:elems) {
            System.out.println(t.toString());
        }
    }
}
