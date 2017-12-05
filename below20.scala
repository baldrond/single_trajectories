package single_trajectories

import java.io.{File, PrintWriter}

import breeze.linalg.DenseVector
import org.apache.spark.{SparkConf, SparkContext}


object below20 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Stavanger").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val listings = sc.textFile(paths.getPath()+"Stavanger_one_week.csv")
    val listingsRDD = listings.map( line => line.split(";"))
    val listingsValues = listingsRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val removeBelow = listingsValues.map(row => (row(1), row(2), row(3), if (row(4).contains("Below")) 10 else row(4).toInt, row(5)))

    //val test = listingsValues.map(row=> row(1)).distinct().count()

   // println(test)

    var day = 0
    var min = 0
    var sec = 0

    var index = 0

    val initMap = removeBelow.map(row => (row._1, makeVector(row._5, row._4)))

    val reduceMap = initMap.reduceByKey((a,b)=> a + b)



    //val toPrint = reduceMap.map(row => row._1 + ";" + row._2).collect()



    val pw = new PrintWriter(new File(paths.getPath()+"below20vector.csv"))
    for(line <- reduceMap.collect()){
      pw.write(line._1)
      for( elem <- line._2){
        pw.write(";" + elem)
      }
      pw.write("\n")
    }

    pw.close

  }

  def makeVector(date: String, value: Int): DenseVector[Int] = {
    val day = date.substring(8,10).toInt
    val min = date.substring(11,13).toInt
    val sec = date.substring(14,16).toInt
    val index = (day - 25)*(60*24)+(min*60)+sec

    val denseVector = DenseVector.zeros[Int](7200)
    denseVector.update(index, value)

    return denseVector
  }
}

