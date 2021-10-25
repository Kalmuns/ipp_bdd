package ipp_bdd;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
//import org.apache.commons.lang3.{JavaVersion, SystemUtils};


import java.util.Arrays;
import java.util.List;


public class Test_spark {
	 
	public Test_spark() {
		 
        // configure spark
        SparkConf sparkConf = new SparkConf().setAppName("Read_file").setMaster("local");
        
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
         
        // provide path to input text file
        //String path = "ipp_bdd/src/50Mo/customer.tbl";
         
        // read text file to RDD
        //JavaRDD<String> lines = sc.textFile(path);
        List<String> data = Arrays.asList("Learn","Apache");
        JavaRDD<String> items = sc.parallelize(data,1);
         
        // collect RDD for printing
        items.foreach(item -> {
            System.out.println("* "+item); 
        });
	}
}