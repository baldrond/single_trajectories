package single_trajectories

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

/*
Our runnable class for calculating the single trajectories of the aggregated data.
Important that the divide_dataset, and network objects have ran before, so that the csv-files have been created.
Must also have a object paths like this:

package single_trajectories

object paths {
  def getPath(): String ={
    <<path to Stavanger_one_week.csv>>
  }
}

 */


object single_trajectories {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Stavanger").setMaster("local[*]")//.set("spark.driver.memory", "8g").set("spark.executor.memory", "8g")
    val sc = new SparkContext(conf)

    val rawfile = sc.textFile(paths.getPath()+"forsteTimen.csv")//.sample(false, 0.3)
    val rawfileRDD = rawfile.map(line => line.split(";")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    //0. Circle name
    //1. cell ID
    //2. easting
    //3. northing
    //4. count
    //5. date

    //Map data from dataset
    val cell_with_coords = rawfileRDD.map(row => (row(1), (row(2).toDouble, row(3).toDouble))).distinct().collect()
    val p = rawfileRDD.filter(row => row(5).equals("2017-09-25 00:00:00")).map(row => (row(1), if (row(4).contains("Below")) 10 else row(4).toInt)).collect()
    val p2 = rawfileRDD.filter(row => row(5).equals("2017-09-25 00:01:00")).map(row => (row(1), if (row(4).contains("Below")) 10 else row(4).toInt)).collect()

    //Load in Network
    val rawfile_network = sc.textFile(paths.getPath()+"network.csv")
    val rawfileRDD_network = rawfile_network.map(line => line.split(";")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val network = rawfileRDD_network.map(row => (((row(0).toDouble, row(1).toDouble), (row(2).toDouble, row(3).toDouble)), (row(4).toDouble, row(5).toDouble)))

    //Create distance matrix
    var dist_matrix_retur = cost_matrix.makeDistMatrix(cell_with_coords)
    var dist_matrix = dist_matrix_retur._1
    var dist_map = dist_matrix_retur._2
    dist_matrix_retur = null

    val cost_matrix_size = cost_matrix.approxMatrixEntries(p, p2.length, dist_matrix)
    println("Creating cost matrix with approximatly density of "+cost_matrix_size._2+" / "+cost_matrix_size._1+" elements.")

    //Create cost matrix
    var coordinate_matrix_retur = cost_matrix.makeCoordinateMatrix(p, p2, dist_matrix, dist_map)
    var single_trajectories = coordinate_matrix_retur._3
    var matrix_entries = coordinate_matrix_retur._1
    val columns_number = coordinate_matrix_retur._2
    var coordinate_matrix: CoordinateMatrix = new CoordinateMatrix(sc.parallelize(matrix_entries))

    coordinate_matrix_retur = null
    dist_matrix = null
    dist_map = null

    println("Starting the hungarian algorithm")
    //Running through the hungarian algorithm
    //step 1
    var step1_retur = hungarian_algorithm.step1(coordinate_matrix.toIndexedRowMatrix())
    if(step1_retur._1){
      coordinate_matrix = step1_retur._2.toCoordinateMatrix()
    }

    step1_retur = null
    //step 2: Not implemented yet as all columns will in the test example will contain at least one 1.0 value
    //TODO: Step 2

    val number_of_iterations = 10
    var sum_list = new ListBuffer[Int]
    //Repeat steps 3–4 until an assignment is possible (In this test: only a few iterations)
    for(i <- 0 until number_of_iterations) {
      println("Iteration "+(i+1))
      println("Step 3")
      //step 3
      var step3_retur = hungarian_algorithm.step3(coordinate_matrix, matrix_entries, columns_number)
      var rows = step3_retur._1
      var columns = step3_retur._2
      step3_retur = null
      println("Step 3-4")
      val highest = hungarian_algorithm.step3_4(coordinate_matrix.toIndexedRowMatrix(), rows)

      val before = coordinate_matrix.entries.filter(entry => entry.i.toInt == highest._2._1 && entry.j.toInt == highest._2._2).first()
      //step 4
      println("Step 4")
      matrix_entries = hungarian_algorithm.step4(coordinate_matrix, rows, highest._1)
      var sum = 0
      for(r <- rows){
        if(r == 1){
          sum += 1
        }
      }
      sum_list += sum
      println("Sum "+(i+1)+": "+sum)
      rows = null
      columns = null
      coordinate_matrix = new CoordinateMatrix(sc.parallelize(matrix_entries))
      val after = coordinate_matrix.entries.filter(entry => entry.i.toInt == highest._2._1 && entry.j.toInt == highest._2._2).first()

      //Printing for test
      print("Before: ")
      println(before)
      print("After: ")
      println(after)
    }

    for((sum, i) <- sum_list.zipWithIndex){
      println("Sum "+i+": "+sum)
    }
  }
}
