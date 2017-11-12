package single_trajectories

import breeze.linalg.DenseVector

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Our approach for dealing with the hungarian algorithm on a aggregated dataset
  */

object hungarian_algorithm {

  //Step 1 - Remove all 1's from rows
  //TODO: Test that this actually works
  def step1(matrix_entries: ListBuffer[Matrix_entry], numRows: Int): ListBuffer[Matrix_entry] = {
    val rows = DenseVector.zeros[Double](numRows)
    for(entry <- matrix_entries) {
      if(rows(entry.i) != -1.0) {
        if (entry.value == 1.0 || entry.value == 2.0) {
          rows(entry.i) = -1.0
        } else {
          if (rows(entry.i) != -1.0 && rows(entry.i) < entry.value) {
            rows(entry.i) = entry.value
          }
        }
      }
    }

    for(entry <- matrix_entries) {
      if(rows(entry.i) != -1.0){
        entry.addToValue(1.0 - rows(entry.i))
      }
    }

    return matrix_entries
  }

  def step2(matrix_entries: ListBuffer[Matrix_entry], numCols: Int): ListBuffer[Matrix_entry] = {
    val cols = DenseVector.zeros[Double](numCols)
    for(entry <- matrix_entries) {
      if(cols(entry.j) != -1.0) {
        if(entry.value == 1.0 || entry.value == 2.0) {
          cols(entry.j) = -1.0
        } else {
          if(cols(entry.j) < entry.value) {
            cols(entry.j) = entry.value
          }
        }
      }
    }

    for(entry <- matrix_entries) {
      if(cols(entry.j) != -1.0){
        entry.addToValue(1.0 - cols(entry.j))
      }
    }

    return matrix_entries
  }

  def step3(matrix_entries: ListBuffer[Matrix_entry], columns_number: ArrayBuffer
    [Int], numRows: Int, numCols: Int): (DenseVector[Boolean], DenseVector[Boolean]) = {
    val columns = DenseVector.zeros[Boolean](numCols)
    val rows = DenseVector.zeros[Boolean](numRows)
    val columns_counter = DenseVector.zeros[Int](numCols)
    val rows_counter = Array.tabulate[ListBuffer[Matrix_entry]](numRows)(elem => new ListBuffer[Matrix_entry])
    val one_entries = new ListBuffer[Matrix_entry]

    for(entry <- matrix_entries){
      if(entry.value == 1.0 || entry.value == 2.0){
        rows_counter(entry.i) += entry
        one_entries += entry
      }
    }
    val rows_counter_sorted = rows_counter
              .filter(list => list.nonEmpty)
              .map(list => list.sortBy(entry => -entry.value))
              .sortBy(list => list.length)

    for(row <- rows_counter_sorted){
      for(entry <- row){
        if(rows(entry.i) || columns_counter(entry.j) != columns_number(entry.j)) {
          columns_counter(entry.j) += 1
          rows(entry.i) = true
        }
      }
      if(!rows(row.head.i)){
        for(entry <- row) {
          columns(entry.j) = true
        }
      }
    }
    for(entry <- one_entries) {
      if(columns(entry.j) && rows(entry.j)){
        rows(entry.j) = false
      }
    }

    return (rows, columns)
  }

  def step4(matrix_entries: ListBuffer[Matrix_entry], rows: DenseVector[Boolean], columns: DenseVector[Boolean]): (ListBuffer[Matrix_entry]) = {
    var highest = 0.0
    for(entry <- matrix_entries){
      if(entry.value != 1.0 && entry.value != 2.0 && !rows(entry.i) && !columns(entry.j)){
        if (entry.value > highest){
          highest = entry.value
        }
      }
    }

    for(entry <- matrix_entries){
      if(entry.value != 1.0 && entry.value != 2.0) {
        if (!rows(entry.i) && !columns(entry.j)) {
          entry.addToValue(1.0 - highest)
        }
      }
      if (columns(entry.j) && rows(entry.i)) {
        entry.addToValue(-(1.0 - highest))
      }
    }
    return matrix_entries
  }

  def get_assignments(matrix_entries: ListBuffer[Matrix_entry], columns_number: ArrayBuffer[Int], numRows: Int, numCols: Int): DenseVector[Int] = {
    val rows = DenseVector.tabulate[Int](numRows)(row => -1)
    val columns_counter = DenseVector.zeros[Int](numCols)
    val rows_counter = Array.tabulate[ListBuffer[Matrix_entry]](numRows)(elem => new ListBuffer[Matrix_entry])

    for(entry <- matrix_entries){
      if(entry.value == 1.0 || entry.value == 2.0){
        rows_counter(entry.i) += entry
      }
    }
    val rows_counter_sorted = rows_counter
      .filter(list => list.nonEmpty)
      .map(list => list.sortBy(entry => -entry.value))
      .sortBy(list => list.length)

    for(row <- rows_counter_sorted){
      for(entry <- row){
        if(rows(entry.i) != -1 || columns_counter(entry.j) != columns_number(entry.j)) {
          columns_counter(entry.j) += 1
          rows(entry.i) = entry.j
        }
      }
    }
    return rows
  }
}