package single_trajectories

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import uk.me.jstott.jcoord.UTMRef

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
    val conf = new SparkConf().setAppName("Stavanger").setMaster("local[*]")
    //.set("spark.driver.memory", "8g").set("spark.executor.memory", "8g")
    val sc = new SparkContext(conf)
    val filename = paths.getPath() + "single_trajectories_0001_3.csv"
    val daytime = true
    val pw = new BufferedWriter(new FileWriter(new File(filename)))
    pw.close()

    var rawfile = sc.textFile(paths.getPath() + "second_hour.csv")
    var rawfileRDD = rawfile.map(line => line.split(";"))
    //0. Circle name
    //1. cell ID
    //2. easting
    //3. northing
    //4. count
    //5. date

    //Map data from dataset
    var cell_with_coords = rawfileRDD.map(row => (row(1), (row(2).toDouble, row(3).toDouble))).distinct().collect()
    val p_values = rawfileRDD.map(row => ((row(5), row(1)), if (row(4).contains("Below")) 10 else row(4).toInt)).reduceByKey((a, b) => a + b)
      .map(row => (row._1._1, addToList(new ListBuffer[(String, Int)], (row._1._2, row._2))))
      .reduceByKey((a, b) => a ++ b).sortByKey().map(row => row._2.toArray).collect()

    rawfile = null
    rawfileRDD = null

    val print = true
    val print_stats = true
    val not_stats = false

    //Load in Network
    var rawfile_network = sc.textFile(paths.getPath() + "network.csv")
    var rawfileRDD_network = rawfile_network.map(line => line.split(";"))
    var network_RDD = rawfileRDD_network.map(row => (((row(0).toDouble, row(1).toDouble), (row(2).toDouble, row(3).toDouble)), (row(4).toDouble, row(5).toDouble)))
    val max_distance = network_RDD.map(row => ("max", row._2._1)).reduceByKey((a, b) => Math.max(a, b)).first()._2
    var network = network_RDD.collect()
    rawfile_network = null
    rawfileRDD_network = null

    //Create distance matrix
    var dist_matrix_retur = cost_matrix.makeDistMatrix(cell_with_coords, network, max_distance)
    var dist_matrix = dist_matrix_retur._1
    var dist_map = dist_matrix_retur._2
    val cell_map = dist_matrix_retur._3
    dist_matrix_retur = null
    network = null
    network_RDD = null
    //Create cost matrixmatrix
    var time = System.currentTimeMillis()
    var coordinate_matrix_retur = cost_matrix.makeCoordinateMatrix(p_values(0), p_values(1), dist_matrix, dist_map, 0)
    println(System.currentTimeMillis() - time)
    var ind = 0
    for (pv <- p_values) {
      var sum = 0
      for (p <- pv) {
        sum += p._2
      }
      println("SUM"+ind+": "+sum)
      ind += 1
    }
    var single_trajectories = coordinate_matrix_retur._4
    var matrix_entries = coordinate_matrix_retur._1
    var columns_number = coordinate_matrix_retur._2
    var columns_name = coordinate_matrix_retur._3
    var row_length = coordinate_matrix_retur._5

    if(print) {
      println(matrix_entries.length)
      println(single_trajectories.length)
      println(columns_number.length)
      println(columns_name.length)
      println(row_length)
    }

    cell_with_coords = null
    coordinate_matrix_retur = null
    //val cost_matrix_size = cost_matrix.approxMatrixEntries(p, p2.length, dist_matrix)
    //println("Creating cost matrix with approximatly density of "+cost_matrix_size._2+" / "+cost_matrix_size._1+" elements.")
    try {
      for (time_step <- 0 until 58) {
        if (print_stats) {
          println("Timestep " + time_step)
        }
        if (time_step != 0) {
          //Create cost matrix
          time = System.currentTimeMillis()
          var coordinate_matrix_retur2 = cost_matrix.makeCoordinateMatrix(p_values(time_step), p_values(time_step + 1), dist_matrix, dist_map, single_trajectories, time_step)
          println("TIME: " + (System.currentTimeMillis() - time))
          single_trajectories = coordinate_matrix_retur2._4
          matrix_entries = coordinate_matrix_retur2._1
          columns_number = coordinate_matrix_retur2._2
          columns_name = coordinate_matrix_retur2._3
          row_length = coordinate_matrix_retur2._5
          var hall_of_fame = coordinate_matrix_retur2._6
          coordinate_matrix_retur2 = null

          if(print) {
            println(matrix_entries.length)
            println(single_trajectories.length)
            println(columns_number.length)
            println(columns_name.length)
            println(row_length)
          }

          printList(hall_of_fame, filename)
          hall_of_fame = null
        }

        if (print) {
          println("Starting the hungarian algorithm")
        }
        //Running through the hungarian algorithm

        //Do early assignments to ease up the problem
        if (print) {
          println("Early assignments")
        }
        var early_assignments_retur = hungarian_algorithm.early_assignments(matrix_entries, columns_number, row_length, columns_number.length)
        var row_matrix = early_assignments_retur._1
        columns_number = early_assignments_retur._2
        var assignments = early_assignments_retur._3
        early_assignments_retur = null
        row_length = row_matrix.length
        var best_assigning: ListBuffer[(Int, Int)] = null

        //Must be tested properly
        row_matrix = hungarian_algorithm.step1(row_matrix)
        //Repeat steps 3–4 until an assignment is possible (In this test: only a few iterations)
        var assignments_left = true
        var i = 0
        while (assignments_left) {
          if (print) {
            println("\nIteration " + (i + 1))
          }
          //Step 3
          var (rows, cols, assigning) = hungarian_algorithm.step3(row_matrix, columns_number, row_length, columns_number.length, print)

          if (assigning != null) {
            best_assigning = new ListBuffer[(Int, Int)]
            for ((row, k) <- row_matrix.zipWithIndex) {
              val a = assigning.valueAt(k)
              val entry = (row._1, a)
              best_assigning += entry
            }
            assignments_left = false
          } else {
            //Step 4
            row_matrix = hungarian_algorithm.step4(row_matrix, rows, cols)
          }
          rows = null
          cols = null
          i += 1
        }
        if (print) {
          println("Found assignments!")
        }
        for (assignment <- best_assigning) {
          assignments(assignment._1) = assignment._2
        }

        i = 0
        for (a <- assignments) {
          if (a == -1) {
            println("ERROR ---> A: " + a + "; I: " + i)
          }
          i += 1
        }

        println("Adding assignments to single trajectories")
        single_trajectories = single_trajectories.map(trajectory =>
          (trajectory._1, addToTrajectory(trajectory._2, columns_name(assignments(trajectory._1))), trajectory._3)
        )

        matrix_entries = null
        columns_number = null
        columns_name = null
        row_matrix = null
        assignments = null
        best_assigning = null

        if ((print || print_stats) && !not_stats) {

          var index = 0
          var moving = 0
          var nearest = 0
          var out = 0
          for (trajectory <- single_trajectories) {
            //println(index)
            if (trajectory._2.distinct.length != 1) {
              val (north1, east1) = cell_map(trajectory._2(trajectory._2.length - 2))
              val lnglat1 = new UTMRef(north1, east1, 'N', 33).toLatLng
              moving += 1
              if (trajectory._2.last.equals("Out")) {
                out += 1
              } else {
                val (north2, east2) = cell_map(trajectory._2.last)
                val lnglat2 = new UTMRef(north2, east2, 'N', 33).toLatLng
                val id1 = dist_map(trajectory._2(trajectory._2.length - 2))
                val id2 = dist_map(trajectory._2.last)
                val dist = dist_matrix(id1, id2)
                if (dist == 0.95) {
                  nearest += 1
                }
              }
            }
            index += 1
          }
          println("Moving / Index = " + (moving.toDouble / index.toDouble))
          println("Nearest / Index = " + (nearest.toDouble / index.toDouble))
          println("Nearest / Moving = " + (nearest.toDouble / moving.toDouble))
          println("Out / Moving = " + (out.toDouble / moving.toDouble) + "\n")
        }
      }
    } catch {
      case e: Exception =>
        println(e)
        printList(single_trajectories, filename)
      System.exit(0)
    }

    // Finnished, print to file
    printList(single_trajectories, filename)

  }

  def printList(single_trajectories: ListBuffer[(Int, ListBuffer[String], Int)], filename: String): Unit = {
    val pw = new BufferedWriter(new FileWriter(new File(filename),true))
    for(trajectory <- single_trajectories){
      pw.write(trajectory._1+";"+trajectory._3+";")
      for(pos <- trajectory._2){
        pw.write(pos+";")
      }
      pw.write("\n")
    }
    pw.close()
  }

  def addToTrajectory(listBuffer: ListBuffer[String], value: String): ListBuffer[String] = {
    listBuffer += value
    return listBuffer
  }

  def addToList(listBuffer: ListBuffer[(String, Int)], value: (String, Int)): ListBuffer[(String, Int)] = {
    listBuffer += value
    return listBuffer
  }
}
