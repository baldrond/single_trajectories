package single_trajectories

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Our approach for dealing with the hungarian algorithm on a aggregated dataset
  */

object hungarian_algorithm {

  //Step 1 - Remove all 1's from rows
  //TODO: Test that this actually works
  def step1(irow_matrix: IndexedRowMatrix): (Boolean, IndexedRowMatrix) = {
    var edit = false
    for(irow <- irow_matrix.rows){
      val array = irow.vector.toArray
      if(!(array contains 1.0)){
        edit = true
        val highest = irow.vector.argmax
        println(irow.index+": "+highest)
        irow.vector.foreachActive((index, value) => (index, value/irow.vector(highest)))
      }
    }

    return (edit, irow_matrix)
  }

  //TODO: Step 2, the same as step 1 just for columns instead of rows

  //Step 3 - Assign 1's
  def step3(coordinate_matrix: CoordinateMatrix, matrix_entries: ListBuffer[MatrixEntry], columns_number: ListBuffer[Int]): (DenseVector[Int], DenseVector[Int]) = {
    val columns = DenseVector.zeros[Int](coordinate_matrix.numCols().toInt)
    val rows = DenseVector.zeros[Int](coordinate_matrix.numRows().toInt)

    for(entry <- matrix_entries) {
      if (entry.value == 1) {
        if (columns(entry.j.toInt) != columns_number(entry.j.toInt)) {
          columns(entry.j.toInt) += 1
          rows(entry.i.toInt) = 1
        }
      }
    }

    return (rows, columns)
  }

  //Step 3 part 2, find highest value
  //TODO: Must improve as columns with not 1, who still are marked on columns, are also counted
  def step3_highest(irow_matrix: IndexedRowMatrix, rows: DenseVector[Int]): (Double, (Int, Int)) = {
    var highest = (0.0, (0,0))
    for(irow <- irow_matrix.rows.collect()){
      if(rows(irow.index.toInt) == 0){
        val array = irow.vector.toArray
        for((elem, i) <- array.zipWithIndex) {
          if (elem > highest._1 && elem != 1.0) {
            highest = (elem, (irow.index.toInt, i))
          }
        }
      }
    }
    return highest
  }

  //Step 4, edit all elements not marked
  //TODO: Must also edit double marked elements
  def step4(coordinate_matrix: CoordinateMatrix, rows: DenseVector[Int], highest: Double): CoordinateMatrix = {
    val new_coordinate_matrix = new CoordinateMatrix(coordinate_matrix.entries.map(entry =>
      MatrixEntry(entry.i, entry.j, if(rows(entry.i.toInt) == 0) Math.min(1.0,entry.value + 1-highest) else entry.value)
    ))
    return new_coordinate_matrix
  }
}
