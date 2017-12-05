package single_trajectories

import breeze.linalg.DenseVector

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Our approach for dealing with the hungarian algorithm on a aggregated dataset
  */

object hungarian_algorithm {

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
      if(cols(entry.j) != -1.0 && entry.j != numCols - 1){
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

    return (new_matrix, columns_number, rows)
  }

  def step3(row_matrix: Array[(Int, ListBuffer[Matrix_entry])], columns_number: ArrayBuffer[Int], numRows: Int, numCols: Int, print: Boolean):
            (DenseVector[Boolean], DenseVector[Boolean], DenseVector[Int]) = {

    val rows = new DenseVector[Boolean](numRows)
    val cols = new DenseVector[Boolean](numCols)
    val verticals = DenseVector.zeros[Int](numCols)
    var assignments = DenseVector.tabulate[Int](numRows)(row => -1)
    val columns_number_copy = columns_number.clone()
    val col_matrix = Array.tabulate[ListBuffer[(Int, Matrix_entry)]](numCols)(elem => new ListBuffer[(Int, Matrix_entry)])

    var row_num = 0
    for(row <- row_matrix){
      for(entry <- row._2){
        if(entry.value == 1.0 || entry.value == 2.0){
          verticals(entry.j) += 1
          val col_entry = (row_num, entry)
          col_matrix(entry.j) += col_entry
        }
      }
      row_num += 1
    }
    row_num = 0
    val empty_rows = new ListBuffer[Int]
    for(row <- row_matrix) {
      val row_entries = new ListBuffer[(Matrix_entry)]
      for (entry <- row._2) {
        if ((entry.value == 1.0 || entry.value == 2.0) && verticals(entry.j) != 0 && columns_number_copy(entry.j) != 0) {
          var vertical_value = columns_number_copy(entry.j).toDouble / verticals(entry.j).toDouble
          if(entry.j == numCols - 1){
            vertical_value = 0
          }
          row_entries += new Matrix_entry(entry.i, entry.j, vertical_value)
        }
      }
      if (row_entries.nonEmpty) {
        val first_row_entry = row_entries.sortBy(entry => -Math.abs(entry.value)).head
        rows(row_num) = true
        if(columns_number_copy(first_row_entry.j) > 0) {
          columns_number_copy(first_row_entry.j) -= 1
        }
        for (entry <- row._2) {
          if(entry.value == 1.0 || entry.value == 2.0) {
            verticals(first_row_entry.j) -= 1
          }
        }
        assignments(row_num) = first_row_entry.j
      } else {
        empty_rows += row_num
      }
      row_num += 1
    }
    for(empty_row <- empty_rows){
      for(row_entry <- row_matrix(empty_row)._2){
        if (row_entry.value == 1.0 || row_entry.value == 2.0){
          cols(row_entry.j) = true
        }
      }
    }
    for((col, col_num) <- col_matrix.zipWithIndex) {
      if(cols(col_num)){
        for((row_num, entry) <- col){
          rows(row_num) = false
        }
      }
    }
    var assign = true
    var rows_missing = 0
    for(row <- rows){
      if(!row){
        assign = false
        rows_missing += 1
      }
    }
    if(!assign){
      assignments = null
    }
    if(print) {
      println("Assignings left: " + empty_rows.length + " (" + rows_missing + ")")
    }

    return (rows, cols, assignments)
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
