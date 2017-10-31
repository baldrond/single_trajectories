package single_trajectories

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

object del_opp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Stavanger").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rawfile = sc.textFile("D:\\Stavanger_one_week\\Stavanger_one_week.csv")
    val rawfileRDD = rawfile.filter(row => row.contains("2017-09-25 00:"))

    val pw = new PrintWriter(new File("D:\\Stavanger_one_week\\forsteTimen.csv"))
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