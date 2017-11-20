package single_trajectories

import breeze.linalg.DenseVector

import scala.collection.mutable
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
          var effective_value = entry.value
          if(effective_value > 1){
            effective_value -= 1.0
          }
          if(rows(entry.i) < effective_value) {
            rows(entry.i) = effective_value
          }
        }
      }
    }

    for(entry <- matrix_entries) {
      if(rows(entry.i) != -1.0){
        entry.addToValue(rows(entry.i))
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
          var effective_value = entry.value
          if(effective_value > 1){
            effective_value -= 1.0
          }
          if(cols(entry.j) < effective_value) {
            cols(entry.j) = effective_value
          }
        }
      }
    }

    for(entry <- matrix_entries) {
      if(cols(entry.j) != -1.0){
        entry.addToValue(cols(entry.j))
      }
    }

    return matrix_entries
  }

  def early_assignments(matrix_entries: ListBuffer[Matrix_entry], columns_number: ArrayBuffer[Int], numRows: Int, numCols: Int):
  (Array[(Int, ListBuffer[Matrix_entry])], ArrayBuffer[Int], DenseVector[Int]) = {
    val rows = DenseVector.tabulate[Int](numRows)(row => -1)
    val all_entries = Array.tabulate[ListBuffer[Matrix_entry]](numRows)(elem => new ListBuffer[Matrix_entry])
    val two_entries = new ListBuffer[Matrix_entry]
    val extra_two_entries = new ListBuffer[Matrix_entry]

    for(entry <- matrix_entries){
      if(entry.value == 2.0){
        two_entries += entry
      } else {
        all_entries(entry.i) += entry
      }
    }

    val columns_number_original = columns_number.clone()
    var new_numRows = numRows
    for(entry <- two_entries){
      if(rows(entry.i) == -1) {
        if (columns_number(entry.j) > columns_number_original(entry.j).toDouble*0.1
        ) {
          rows(entry.i) = entry.j
          columns_number(entry.j) -= 1
          new_numRows -= 1
        } else {
          extra_two_entries += entry
        }
      }
    }

    for(entry <- extra_two_entries){
      all_entries(entry.i) += entry
    }

    val new_matrix = new Array[(Int, ListBuffer[Matrix_entry])](new_numRows)

    var i = 0
    var new_i = 0
    for(r <- rows){
      if(r == -1){
        new_matrix(new_i) = (i, all_entries(i))
        new_i += 1
      }
      i += 1
    }

    println(new_numRows)

    return (new_matrix, columns_number, rows)
  }

  def step3(row_matrix: Array[(Int, ListBuffer[Matrix_entry])], columns_number: ArrayBuffer[Int], numRows: Int, numCols: Int):
            (DenseVector[Boolean], DenseVector[Boolean], DenseVector[Int]) = {

    val assignments = DenseVector.tabulate[Int](numRows)(row => -1)
    val rows = new DenseVector[Boolean](numRows)
    val cols = new DenseVector[Boolean](numCols)
    val verticals = DenseVector.zeros[Int](numCols)
    val horisontals = DenseVector.zeros[Int](numRows)

    val colrows = new ListBuffer[(Matrix_entry, Int)]


    for(row <- row_matrix){
      for(entry <- row._2){
        if(entry.value == 1.0 || entry.value == 2.0){
          verticals(entry.j) += 1
        }
      }
    }
    var row_num = 0
    for(row <- row_matrix){
      horisontals(row_num) = findHorisontalZeros(row_matrix, row_num)
      for(entry <- row._2){
        if(entry.value == 1.0 || entry.value == 2.0){
          val vertical_value = verticals(entry.j).toDouble / columns_number(entry.j).toDouble
          val buffer_entry = (mostColsOrRows(horisontals(row_num), vertical_value, entry), row_num)
          colrows += buffer_entry
        }
      }
      row_num += 1
    }

    for((entry, row_num) <- colrows) {
      if(!(cols(entry.j) || rows(row_num))){
        if (entry.value > 0.0) {
          cols(entry.j) = true
        } else if (entry.value < 0.0) {
          rows(row_num) = true
          assignments(row_num) = entry.j
        }
      }
    }
    return (rows, cols, assignments)
  }

  def findHorisontalZeros(row_matrix: Array[(Int, ListBuffer[Matrix_entry])], row_num: Int): Int ={
    var count = 0
    for(entry <- row_matrix(row_num)._2){
      if(entry.value == 1.0 || entry.value == 2.0){
        count += 1
      }
    }
    return count
  }

  def mostColsOrRows(horisontal: Double, vertical: Double, selected_entry: Matrix_entry): Matrix_entry = {
    var value = 0.0
    if(vertical > horisontal){
      value = vertical
    } else {
      value = -horisontal
    }
    return new Matrix_entry(selected_entry.i, selected_entry.j, value)
  }

  def step4(row_matrix: Array[(Int, ListBuffer[Matrix_entry])], rows: DenseVector[Boolean], columns: DenseVector[Boolean]): Array[(Int, ListBuffer[Matrix_entry])] = {
    var highest = 0.0
    var next_highest = 0.0
    var i = 0
    for(row <- row_matrix){
      for(entry <- row._2) {
        if (entry.value != 1.0 && entry.value != 2.0 && !rows(i) && !columns(entry.j)) {
          var effective_value = entry.value
          if (effective_value > 1) {
            effective_value -= 1.0
          }
          if (effective_value > highest) {
            next_highest = highest
            highest = effective_value
          } else if(effective_value > next_highest && effective_value < highest){
            next_highest = effective_value
          }
        }
      }
      i += 1
    }

    println("Highest: "+highest)
    println("Next Highest: "+next_highest)

    i = 0
    for(row <- row_matrix){
      for(entry <- row._2) {
        if (entry.value != 1.0 && entry.value != 2.0) {
          if (!rows(i) && !columns(entry.j)) {
            entry.addToValue(highest)
          }
        }
        if (columns(entry.j) && rows(i)) {
          entry.subtractFromValue(highest)
        }
      }
      i += 1
    }
    return row_matrix
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
