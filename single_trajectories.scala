package single_trajectories

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.{SparkConf, SparkContext}

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

    //Create distance matrix
    val dist_matrix_retur = cost_matrix.makeDistMatrix(cell_with_coords)
    val dist_matrix = dist_matrix_retur._1
    val dist_map = dist_matrix_retur._2

    //Create cost matrix
    val coordinateMatrix_retur = cost_matrix.makeCoordinateMatrix(p, p2, dist_matrix, dist_map)
    var single_trajectories = coordinateMatrix_retur._3
    var matrix_entries = coordinateMatrix_retur._1
    val columns_number = coordinateMatrix_retur._2
    var coordinate_matrix: CoordinateMatrix = new CoordinateMatrix(sc.parallelize(matrix_entries))

    //Running through the hungarian algorithm
    //step 1
    val step1_retur = hungarian_algorithm.step1(coordinate_matrix.toIndexedRowMatrix())
    if(step1_retur._1){
      coordinate_matrix = step1_retur._2.toCoordinateMatrix()
    }
    //step 2: Not implemented yet as all columns will in the test example will contain at least one 1.0 value
    //TODO: Step 2

    val number_of_iterations = 10
    //Repeat steps 3–4 until an assignment is possible (In this test: only a few iterations)
    for(i <- 0 until number_of_iterations) {
      //step 3
      val step3_retur = hungarian_algorithm.step3(coordinate_matrix, matrix_entries, columns_number)
      val rows = step3_retur._1
      var columns = step3_retur._2
      val highest = hungarian_algorithm.step3_highest(coordinate_matrix.toIndexedRowMatrix(), rows)

      val before = coordinate_matrix.entries.filter(entry => entry.i.toInt == highest._2._1 && entry.j.toInt == highest._2._2).first()
      //step 4
      matrix_entries = hungarian_algorithm.step4(coordinate_matrix, rows, highest._1)
      coordinate_matrix = new CoordinateMatrix(sc.parallelize(matrix_entries))
      val after = coordinate_matrix.entries.filter(entry => entry.i.toInt == highest._2._1 && entry.j.toInt == highest._2._2).first()

      //Printing for test
      print("Before: ")
      println(before)
      print("After: ")
      println(after)

    }
  }
}
