package single_trajectories

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
/*
Simple method to divide the dataset to only the first hour, so it will be faster to run.
 */


object divide_dataset {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Stavanger").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rawfile = sc.textFile(paths.getPath()+"Stavanger_one_week_edited.csv")
    val rawfileRDD = rawfile.filter(row => row.contains("2017-09-25 01:"))


    val pw = new PrintWriter(new File(paths.getPath()+"second_hour.csv"))
    for(line <- rawfileRDD.collect()){
      pw.write(line + "\n")
    }

    pw.close
    //0. Circle name
    //1. cell ID
    //2. easting
    //3. northing
    //4. count
    //5. date
  }
}
