package ma.enset.tp1sparkrdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;

public class Application {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("TP1 RDD").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<Double> rdd1=sc.parallelize(Arrays.asList(12.0,11.0));
        JavaRDD<Double> rdd2=rdd1.map(a -> {
            System.out.println("map trans");
            return a+1;
        }  );
        JavaRDD<Double> rdd3=rdd2.filter(a -> a>10 );
        rdd3.persist(StorageLevel.MEMORY_ONLY());
        System.out.println(rdd3.count());
        System.out.println(rdd3.count());


    }
}
