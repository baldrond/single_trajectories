package single_trajectories

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

    var rawfile = sc.textFile(paths.getPath()+"forsteTimen.csv")//.sample(false, 0.3)
    var rawfileRDD = rawfile.map(line => line.split(";"))
    //0. Circle name
    //1. cell ID
    //2. easting
    //3. northing
    //4. count
    //5. date

    //Map data from dataset
    var cell_with_coords = rawfileRDD.map(row => (row(1), (row(2).toDouble, row(3).toDouble))).distinct().collect()
    var p = rawfileRDD.filter(row => row(5).equals("2017-09-25 00:49:00")).map(row => (row(1), if (row(4).contains("Below")) 10 else row(4).toInt)).collect()
    var p2 = rawfileRDD.filter(row => row(5).equals("2017-09-25 00:50:00")).map(row => (row(1), if (row(4).contains("Below")) 10 else row(4).toInt)).collect()
    rawfile = null
    rawfileRDD = null


    //Load in Network
    var rawfile_network = sc.textFile(paths.getPath()+"network.csv")
    var rawfileRDD_network = rawfile_network.map(line => line.split(";"))
    var network_RDD = rawfileRDD_network.map(row => (((row(0).toDouble, row(1).toDouble), (row(2).toDouble, row(3).toDouble)), (row(4).toDouble, row(5).toDouble)))
    val max_distance = network_RDD.map(row => ("max", row._2._1)).reduceByKey((a,b) => Math.max(a,b)).first()._2
    var network = network_RDD.collect()
    rawfile_network = null
    rawfileRDD_network = null

    //Create distance matrix
    var dist_matrix_retur = cost_matrix.makeDistMatrix(cell_with_coords, network, max_distance)
    var dist_matrix = dist_matrix_retur._1
    var dist_map = dist_matrix_retur._2
    dist_matrix_retur = null
    network = null
    network_RDD = null

    //val cost_matrix_size = cost_matrix.approxMatrixEntries(p, p2.length, dist_matrix)
    //println("Creating cost matrix with approximatly density of "+cost_matrix_size._2+" / "+cost_matrix_size._1+" elements.")

    //Create cost matrix
    var coordinate_matrix_retur = cost_matrix.makeCoordinateMatrix(p, p2, dist_matrix, dist_map)
    var single_trajectories = coordinate_matrix_retur._4
    var matrix_entries = coordinate_matrix_retur._1
    var columns_number = coordinate_matrix_retur._2
    val columns_name = coordinate_matrix_retur._3
    var row_length = coordinate_matrix_retur._5

    cell_with_coords = null
    p = null
    p2 = null
    coordinate_matrix_retur = null
    //dist_matrix = null
    //dist_map = null

    println("Starting the hungarian algorithm")
    //Running through the hungarian algorithm

    //Must be tested properly
    //matrix_entries = hungarian_algorithm.step1(matrix_entries, row_length)
    //matrix_entries = hungarian_algorithm.step2(matrix_entries, columns_number.length)

    //Do early assignments to ease up the problem
    println("Early assignments")
    var early_assignments_retur = hungarian_algorithm.early_assignments(matrix_entries, columns_number, row_length, columns_number.length)
    var row_matrix = early_assignments_retur._1
    columns_number = early_assignments_retur._2
    var assignments = early_assignments_retur._3
    early_assignments_retur = null
    row_length = row_matrix.length
    var best_assigning: ListBuffer[(Int, Int)] = null

    //Repeat steps 3–4 until an assignment is possible (In this test: only a few iterations)
    var assignments_left = 100000
    var i = 0
    while(assignments_left != 0) {
      println("\nIteration "+(i+1))
      best_assigning = new ListBuffer[(Int, Int)]
      //Step 3
      var (rows, cols, assigning) = hungarian_algorithm.step3(row_matrix, columns_number, row_length, columns_number.length)

      var sum_row = 0
      var k = 0
      for(r <- rows){
        if(r){
          sum_row += 1
          val entry = (row_matrix(k)._1, assigning(k))
          best_assigning += entry
        }
        k += 1
      }
      assigning = null
      assignments_left = rows.length-sum_row
      println("Assignments left: "+assignments_left)

      //Step 4
      if(rows.length > sum_row) {
        row_matrix = hungarian_algorithm.step4(row_matrix, rows, cols)
      }
      rows = null
      cols = null
      i+=1
    }

    println("Found assignments!")
    for(assignment <- best_assigning){
      assignments(assignment._1) = assignment._2
    }

    i = 0
    for(a <- assignments){
      if(a == -1){
        println("A: "+a+"; I: "+i)
      }
      i += 1
    }
    println("Adding assignments to single trajectories")
    single_trajectories = single_trajectories.map(trajectory =>
      (trajectory._1, addToTrajectory(trajectory._2, columns_name(assignments(trajectory._1))))
    )

    var index = 0
    var moving = 0
    var nearest = 0
    for(trajectory <- single_trajectories){
      if(trajectory._2.distinct.length != 1){
        moving += 1
        val id1 = dist_map(trajectory._2(0))
        val id2 = dist_map(trajectory._2(1))
        val dist = dist_matrix(id1, id2)
        if(dist == 1.0){
          nearest += 1
        }
      }
      index += 1
    }
    println("Moving / Index = "+(moving.toDouble/index.toDouble))
    println("Nearest / Index = "+(nearest.toDouble/index.toDouble))
    println("Nearest / Moving = "+(nearest.toDouble/moving.toDouble))
  }

  def addToTrajectory(listBuffer: ListBuffer[String], value: String): ListBuffer[String] = {
    listBuffer += value
    return listBuffer
  }
}
