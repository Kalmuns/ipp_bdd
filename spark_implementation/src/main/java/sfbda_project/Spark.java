package sfbda_project;
// use RDD : JavaRDD<String> distFile = sc.textFile("data.txt");
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.util.Scanner; 

public class Spark {

    public JavaSparkContext ClusterAccess(String appName, String master){    
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }

    public JavaRDD LoadToRDD(String filename, JavaSparkContext sc){
        JavaRDD<String> distFile = sc.textFile(filename);
        return distFile;
    }

    public JavaRDD Select(JavaRDD DataTableRDD){
        JavaRDD result = DataTableRDD;
        return result;
    }

    public JavaRDD Filter(JavaRDD DataTableRDD){
        JavaRDD result = DataTableRDD;
        return result;

    }

    public JavaRDD HashJoin(JavaRDD DataTableRDD_1, JavaRDD DataTableRDD_2){
        JavaRDD result = DataTableRDD_1;
        return result;
    }

    public void main(String[] args){
        String appName = "sfbda";
        String master = "local";
        String filename = "Path";
        JavaSparkContext sc = ClusterAccess(appName, master);
        LoadToRDD(filename, sc);
    }
}
