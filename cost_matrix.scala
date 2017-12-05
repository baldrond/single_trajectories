package single_trajectories

import java.util

import breeze.linalg.DenseMatrix

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, ListBuffer, MultiMap, Set}

object cost_matrix {

  //Input: a list with all position_id, (northling, eastling)
  //Output: a Matrix with all distances between the positions, and a HashMap which maps id to position in matrix
  def makeDistMatrix(array: Array[(String, (Double, Double))], network: Array[(((Double, Double),(Double,Double)), (Double, Double))], max_distance: Double): (DenseMatrix[Double], HashMap[String, Int], HashMap[String, (Double, Double)]) = {
    var dist_matrix = DenseMatrix.zeros[Double](array.length,array.length)
    var dist_map: HashMap[String, Int] = HashMap()
    var cell_map: HashMap[String, (Double, Double)] = new HashMap()
    val coord_map = new HashMap[(Double, Double), Set[String]] with MultiMap[(Double, Double), String]
    val threshold = 500.0

    val test = new ListBuffer[String]

    for((acell, i) <- array.zipWithIndex){
      val entry = (acell._1, i)
      dist_map += entry
      cell_map += acell
      coord_map.addBinding(acell._2, acell._1)
      for((another, j) <- array.zipWithIndex){
        if(acell._1.equals(another._1)){
          dist_matrix(i,j) = 2.0
        } else if(acell._2.equals(another._2)){
          dist_matrix(i,j) = 1.0
        }
      }
    }
    for(connection <- network){
      val point1 = coord_map.get(connection._1._1)
      val point2 = coord_map.get(connection._1._2)
      for(cell1 <- point1.get){
        val i = dist_map(cell1)
        for(cell2 <- point2.get){
          val j = dist_map(cell2)
          dist_matrix(i, j) = Math.max(1.0 - (connection._2._1/max_distance), 0.00001)
        }
      }
    }

    return (dist_matrix, dist_map, cell_map)
  }
  //Approximately count matrix entries
  def approxMatrixEntries(array1: Array[(String, Int)], array2length: Int, dist_matrix: DenseMatrix[Double]): (Long, Long) = {
    var average_array_1 = 0.0
    for (element <- array1) {
      average_array_1 += element._2
    }
    var dist_matrix_entries = 0.0
    dist_matrix.foreachValue(value => if(value != 0.0) dist_matrix_entries += 1)
    val dist_matrix_density = dist_matrix_entries / (dist_matrix.rows * dist_matrix.cols)
    val total_number = (average_array_1/array1.length.toDouble) * array1.length.toDouble * array2length.toDouble
    val matrix_entries = total_number * dist_matrix_density
    return (Math.round(total_number), Math.round(matrix_entries))
  }

  //Input: Arrays for first and second timestep (position, count), distance matrix and distance map
  //Output: List of matrix entries, List of count on columns, and first iteration of the trajectory, represented as a list of a string-list.
  def makeCoordinateMatrix(array1: Array[(String, Int)], array2: Array[(String, Int)], dist_matrix: DenseMatrix[Double], dist_map: HashMap[String, Int], iteration: Int):
            (ListBuffer[(Matrix_entry)], ArrayBuffer[Int], ArrayBuffer[String], ListBuffer[(Int, ListBuffer[String], Int)], Int) = {
    var s = new ListBuffer[(Int, ListBuffer[String], Int)]
    var matrix_entries = new ListBuffer[Matrix_entry]
    var columns_number = new ArrayBuffer[Int]
    var columns_name = new ArrayBuffer[String]

    var index = 0
    for ((element1, k) <- array1.zipWithIndex) {
      for(i <- 0 until element1._2){
        val s_entry = (index + i, new ListBuffer[String], iteration)
        s_entry._2 += element1._1
        s += s_entry
      }
      for((element2, j) <- array2.zipWithIndex) {
        if(k == 0){
          columns_number += element2._2
          columns_name += element2._1
        }
        var distance = dist_matrix(dist_map(element1._1), dist_map(element2._1))
        if (distance > 0){
          for(i <- 0 until element1._2) {
            matrix_entries += new Matrix_entry(index + i, j, distance)
          }
        }
      }
      index += element1._2
    }
    index = 0
    columns_number += 10000
    columns_name += "Out"
    for(entries <- s){
      matrix_entries += new Matrix_entry(index, columns_number.length - 1, 0.000001)
      index += 1
    }
    return (matrix_entries, columns_number, columns_name, s, index)
  }

  def makeCoordinateMatrix(array1: Array[(String, Int)], array2: Array[(String, Int)], dist_matrix: DenseMatrix[Double], dist_map: HashMap[String, Int], l_t: ListBuffer[(Int, ListBuffer[String], Int)], iteration: Int):
          (ListBuffer[(Matrix_entry)], ArrayBuffer[Int], ArrayBuffer[String], ListBuffer[(Int, ListBuffer[String], Int)], Int, ListBuffer[(Int, ListBuffer[String], Int)]) = {
    var matrix_entries = new ListBuffer[Matrix_entry]
    var columns_number = new ArrayBuffer[Int]
    var columns_name = new ArrayBuffer[String]
    var s_t = new ListBuffer[(Int, ListBuffer[String], Int)]
    val hall_of_fame = new ListBuffer[(Int, ListBuffer[String], Int)]
    var index = 0
    var outcounter = 0
    val s_l_t = l_t.sortWith((a,b) => a._2.last > b._2.last)
    var last_id = s_l_t.head._2.last
    for(trajectory <- s_l_t){
      if(trajectory._2.last.equals("Out")){
        hall_of_fame += trajectory
      } else if(!trajectory._2.last.equals(last_id)){
        for((element2, j) <- array2.zipWithIndex) {
          val distance = dist_matrix(dist_map(last_id), dist_map(element2._1))
          if (distance > 0) {
            for (i <- 0 until outcounter){
              matrix_entries += new Matrix_entry(index + i, j, distance)
            }
          }
        }
        var i = 0
        while (i < array1.length){
          if(array1(i)._1.equals(last_id)) {
            array1(i) = (array1(i)._1, array1(i)._2 - outcounter)
            i = array1.length
          }
          i += 1
        }
        index += outcounter
        val new_entry = (index, trajectory._2, trajectory._3)
        s_t += new_entry
        outcounter = 1
        last_id = trajectory._2.last
      } else {
        val new_entry = (index + outcounter, trajectory._2, trajectory._3)
        s_t += new_entry
        outcounter += 1
      }
    }
    println("hall of fame: "+hall_of_fame.length)
    println("S_T: "+s_t.length)

    for((element2, j) <- array2.zipWithIndex) {
      val distance = dist_matrix(dist_map(last_id), dist_map(element2._1))
      if (distance > 0) {
        for (i <- 0 until outcounter){
          matrix_entries += new Matrix_entry(index + i, j, distance)
        }
      }
    }
    var i = 0
    while (i < array1.length){
      if(array1(i)._1.equals(last_id)) {
        array1(i) = (array1(i)._1, array1(i)._2 - outcounter)
        i = array1.length
      }
      i += 1
    }
    index += outcounter

    for ((element1, k) <- array1.zipWithIndex) {
      for(i <- 0 until element1._2){
        val s_entry = (index + i, new ListBuffer[String], iteration)
        s_entry._2 += element1._1
        s_t += s_entry
      }
      for((element2, j) <- array2.zipWithIndex) {
        if(k == 0){
          columns_number += element2._2
          columns_name += element2._1
        }
        var distance = dist_matrix(dist_map(element1._1), dist_map(element2._1))
        if (distance > 0){
          for(i <- 0 until element1._2) {
            matrix_entries += new Matrix_entry(index + i, j, distance)
          }
        }
      }
      index += element1._2
    }
    columns_number += matrix_entries.length
    columns_name += "Out"
    for(entries <- s_t){
      matrix_entries += new Matrix_entry(entries._1, columns_number.length - 1, 0.000001)
    }
    return (matrix_entries, columns_number, columns_name, s_t, index, hall_of_fame)
  }
}



