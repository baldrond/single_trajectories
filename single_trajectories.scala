package single_trajectories

import org.apache.spark.{SparkConf, SparkContext}

object single_trajectories {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Stavanger").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rawfile = sc.textFile("D:\\Stavanger_one_week\\Stavanger_one_week.csv")
    val rawfileRDD = rawfile.map(line => line.split(";")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    //0. Circle name
    //1. cell ID
    //2. easting
    //3. northing
    //4. count
    //5. date

    val p = rawfileRDD.map(row => ((row(1), row(5)), if(row(4).contains("Below"))10 else row(4).toInt))



  }
}
