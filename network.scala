package single_trajectories

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import uk.me.jstott.jcoord.{LatLng, UTMRef}

import scala.collection.mutable.ListBuffer

/*
Method for creating a network of the datapoints (Calculating neighbors)
TODO: Needs to be more accurate
 */


object network {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Stavanger").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rawfile = sc.textFile("D:\\Stavanger_one_week\\forsteTimen.csv")
    val rawfileRDD = rawfile.map(line => line.split(";")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    //0. Circle name
    //1. cell ID
    //2. easting
    //3. northing
    //4. count
    //5. date

    val onlyPoints = rawfileRDD.map(row => ((row(2).toDouble, row(3).toDouble), 0)).reduceByKey((a,b) => a).collect()

    val offset = 20

    val full_list = new ListBuffer[ListBuffer[(Edge, Double, Double, ((Double, Double),(Double, Double)))]]

    val water_edges = new ListBuffer[Edge]
    water_edges += new Edge(new Point(58.8863775541457, 5.752716064453125), new Point(59.002561207839875, 5.804901123046875))
    water_edges += new Edge(new Point(58.90836909237775, 5.6352996826171875), new Point(58.9345987463464, 5.6710052490234375))
    water_edges += new Edge(new Point(59.001677140812276, 5.689888000488281), new Point(58.996372261836974, 5.732717514038086))
    water_edges += new Edge(new Point(58.94584657900828, 5.61847686767578), new Point(58.97540999755196, 5.645942687988281))
    water_edges += new Edge(new Point(59.01025163301516, 5.68714141845703), new Point(59.025450635798975, 5.755462646484375)) //Åmøy - Hundvåg
    water_edges += new Edge(new Point(58.988634514675105, 5.752286911010742), new Point(58.97249018938895, 5.796318054199218))

    for ((aPoint, i) <- onlyPoints.zipWithIndex){
      val lnglat = new UTMRef(aPoint._1._1, aPoint._1._2, 'N', 33).toLatLng
      val point = new Point(lnglat.getLat, lnglat.getLng)
      println("single_trajectories.Point: "+i)

      val edge_list = new ListBuffer[(Edge, Double, Double, ((Double, Double),(Double, Double)))]
      for ((anotherPoint, j) <- onlyPoints.zipWithIndex){
        if(i != j) {
          val lnglat2 = new UTMRef(anotherPoint._1._1, anotherPoint._1._2, 'N', 33).toLatLng
          val point2 = new Point(lnglat2.getLat, lnglat2.getLng)

          val edge = new Edge(point, point2)
          val distance = edge.distBetween()
          val angle = edge.angleBetween()
          val entry = (edge, distance, angle, (aPoint._1, anotherPoint._1))

          var enter = true
          var water_collision = false

          for (w <- water_edges) {
            if (edge.collides(w)) {
              water_collision = true
              enter = false
            }
          }
          if (!water_collision) {
            for (e <- edge_list) {
              if ((e._3 < (angle + offset) % 360 && e._3 > (angle - offset) % 360) || ((e._3 + 180) % 360 < (angle + 180 + offset) % 360 && (e._3 + 180) % 360 > (angle + 180 - offset) % 360)) {
                if (distance < e._2) {
                  edge_list -= e
                  edge_list += entry
                  enter = false
                } else {
                  enter = false
                }
              }
            }
          }
          if (enter) {
            edge_list += entry
          }
        }
      }
      val distinct_edge_list = edge_list.distinct
      var lavest = 9999999.0
      var hoyest = 0.0
      var sum = 0.0
      for(e <- distinct_edge_list){
        if(e._2 < lavest){
          lavest = e._2
        }
        if(e._2 > hoyest){
          hoyest = e._2
        }
        sum += e._2
      }
      val steg = (hoyest - lavest) / distinct_edge_list.size

      val final_list = new ListBuffer[(Edge, Double, Double, ((Double, Double),(Double, Double)))]
      for(e <- distinct_edge_list){
        if (e._2 <= lavest + steg * 5) {
          final_list += e
          println(e._1.toPrint() + ": " + e._2 + " , " + e._3)
        }
      }
      println("\n")

      full_list += final_list
    }

    val full_list_unedited = full_list

    for((list, i) <- full_list.zipWithIndex){
      for(line <- list){
        var equality = false
        for(index <- i+1 until full_list.length) {
          for (line2 <- full_list(index)) {
            if(line._1.equalsOpposite(line2._1)){
              equality = true
            }
          }
        }
        if(!equality){
          full_list(i) -= line
          if(full_list(i).isEmpty){
            var lowest = (999999999.0, -1)
            for((element, j) <- full_list_unedited(i).zipWithIndex){
              if(element._2 < lowest._1){
                lowest = (element._2, j)
              }
            }
            full_list(i) += line
          }
        }
      }
    }


    //Make csv on the form from_east;from_north;to_east;to_north;distance;angle
    var pw = new PrintWriter(new File("D:\\Stavanger_one_week\\network.csv"))
    for(list <- full_list) {
      for (line <- list) {
        pw.write(line._4._1._1+";"+line._4._1._2+";"+line._4._2._1+";"+line._4._2+";"+line._2+";"+line._3 + "\n")
      }
    }
    pw.close

    //Make GeoJSON
    var geojson = "{\n  \"type\": \"FeatureCollection\",\n  \"features\": ["
    for(list <- full_list){
      for(line <- list){
        geojson += "{\n      \"type\": \"Feature\",\n      \"properties\": {\"distance\": "+line._2+"},\n      \"geometry\": {\n        \"type\": \"LineString\",\n        \"coordinates\": [\n          [\n            "+line._1.p1.lng+",\n            "+line._1.p1.lat+"\n          ],\n          [\n            "+line._1.p2.lng+",\n            "+line._1.p2.lat+"\n          ]\n        ]\n      }\n    },"
      }
    }
    geojson = geojson.substring(0, geojson.length-1) + "]\n}"

    pw = new PrintWriter(new File("D:\\Stavanger_one_week\\network.geojson"))
    pw.write(geojson)
    pw.close()
  }
}
