package single_trajectories

import breeze.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}

import scala.collection.mutable.{HashMap, ListBuffer}

object cost_matrix {

  //Input: a list with all position_id, (northling, eastling)
  //Output: a Matrix with all distances between the positions, and a HashMap which maps id to position in matrix
  def makeDistMatrix(array: Array[(String, (Double, Double))]): (DenseMatrix[Double], HashMap[String, Int]) = {
    var dist_matrix = DenseMatrix.zeros[Double](array.length,array.length)
    var dist_map: HashMap[String, Int] = HashMap()

    for((acell, i) <- array.zipWithIndex){
      val entry = (acell._1, i)
      dist_map += entry

      for ((another, j) <- array.zipWithIndex){
        val east = Math.abs(acell._2._1 - another._2._1)
        val north = Math.abs(acell._2._2 - another._2._2)
        val distance = Math.sqrt(Math.pow(east, 2) + Math.pow(north, 2))
        val threshold = 500.0
        if (distance > threshold) {
          dist_matrix(i, j) = 0.0
        } else {
          dist_matrix(i, j) = 1.0 - (distance/threshold)
        }
      }
    }

    return (dist_matrix, dist_map)
  }

  //Input: Arrays for first and second timestep (position, count), distance matrix and distance map
  //Output: List of matrix entries, List of count on columns, and first iteration of the trajectory, represented as a list of a string-list.
  def makeCoordinateMatrix(array1: Array[(String, Int)], array2: Array[(String, Int)], dist_matrix: DenseMatrix[Double], dist_map: HashMap[String, Int]): (ListBuffer[(MatrixEntry)], ListBuffer[Int], ListBuffer[ListBuffer[String]]) = {
    var s = new ListBuffer[ListBuffer[String]]
    var matrix_entries = new ListBuffer[MatrixEntry]
    var columns_number = new ListBuffer[Int]

    var index = 0
    for ((element1, k) <- array1.zipWithIndex) {
      var s_entry = new ListBuffer[String]
      println("Index: "+index+" ("+element1._2+","+array2.length+")")
      for(i <- 0 until element1._2){
        for((element2, j) <- array2.zipWithIndex) {
          if(k == 0 && i == 0){
            columns_number += element2._2
          }
          val distance = dist_matrix(dist_map(element1._1), dist_map(element2._1))
          if (distance > 0){
            matrix_entries += new MatrixEntry(index, j, distance)
          }
        }
        index += 1
        s_entry += element1._1
      }
      s += s_entry
    }

    return (matrix_entries, columns_number, s)
  }
}



