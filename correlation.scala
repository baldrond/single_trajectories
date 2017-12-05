package single_trajectories

import java.io.{File, PrintWriter}

import breeze.linalg.DenseVector
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{HashMap, ListBuffer}

object correlation {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Stavanger").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rawFile = sc.textFile(paths.getPath()+"Stavanger_one_week.csv")
    val rawFileRDD = rawFile.map( line => line.split(";"))
    val rawFileValues = rawFileRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val idInfo = rawFileValues.map(row => (row(1), row)).reduceByKey((a,b) => a).collect()
    val idMap: HashMap[String, (String, String, String)] = HashMap()
    for(id <- idInfo){
      val map_entry = (id._1, (id._2(0), id._2(2), id._2(3)))
      idMap += map_entry
    }

    val rawFile_B20 = sc.textFile(paths.getPath()+"below20vector.csv")
    val rawRDD_B20 = rawFile_B20.map(line => line.split(";"))

    val idAndCount = rawRDD_B20.map(row=> (row(0), getCounts(row))).collect()

    val noZeroData = rawRDD_B20.map(row=> (row(0), removeZeros(getCounts(row)))).collect()

    val editableData = noZeroData.filter(row => containKtens(row._2, 1.0))
    val newData: HashMap[String, DenseVector[Int]] = HashMap()

    var nr = 0
    for(row <- editableData){
      println(nr + " / "+editableData.length)
      val allSubs = new ListBuffer[(Double, Double, DenseVector[Double])]
      var totalCor = 0.0
      for(row2 <- noZeroData){
        if(row._1 != row2._1){
          val (cor, diff) = correlation(row._2, row2._2)
          if(!cor.isNaN){
            val entry = (cor, diff, findSubs(row._2, row2._2))
            allSubs += entry
            totalCor += cor
          }
        }
      }
      val newVector = DenseVector.zeros[Double](row._2.length)
      var i = 0
      var unknownFirstValue = (false, false)
      var ufvCount = 0
      for(elem <- row._2){
        if(elem != 0 && elem != 10){
          newVector(i) = elem
          if(unknownFirstValue._1){
            unknownFirstValue = (true, true)
          }
        } else {
          if(!unknownFirstValue._2 && unknownFirstValue._1){
            ufvCount += 1
          }
          totalCor = 0.0
          for (sub <- allSubs) {
            if(sub._3(i) != 0.0 && sub._3(i-1) != 0.0) {
              totalCor += sub._1
            }
          }
          var change = 0.0
          if(i > 0) {
            for (sub <- allSubs) {
              if(sub._3(i) != 0.0 && sub._3(i-1) != 0.0) {
                val lastDiff = newVector(i-1) / sub._3(i-1)
                newVector(i) += (sub._3(i) * lastDiff)  * (sub._1 / totalCor)
              }
            }
          } else {
            unknownFirstValue = (true, false)
            ufvCount = 1
          }
          newVector(i) = Math.min(changeValue(newVector(i)), 20.0)
        }
        i += 1
      }
      if(unknownFirstValue._1 && unknownFirstValue._2){
        for(i <- 0 until ufvCount+1){
          totalCor = 0.0
          for (sub <- allSubs) {
            if(sub._3(ufvCount-i) != 0.0 && sub._3(ufvCount-i + 1) != 0.0) {
              totalCor += sub._1
            }
          }
          var change = 0.0
          for (sub <- allSubs) {
            if(sub._3(ufvCount-i) != 0.0 && sub._3(ufvCount-i + 1) != 0.0) {
              val lastDiff = newVector(ufvCount-i+1) / sub._3(ufvCount-i+1)
              newVector(ufvCount-i) += (sub._3(ufvCount-i) * lastDiff)  * (sub._1 / totalCor)
            }
          }
          newVector(ufvCount-i) = Math.min(changeValue(newVector(ufvCount-i)), 20.0)
        }
      }
      val map_entry = (row._1, newVector.map(value => value.toInt))
      newData += map_entry
      nr += 1
    }

    nr = 0
    val pw = new PrintWriter(new File(paths.getPath()+"stavanger_one_week_edited.csv"))
    for(data <- noZeroData){
      println(nr+" / "+noZeroData.length)
      if(newData.isDefinedAt(data._1)){
        val newDataVector = newData(data._1)
        val info = idMap(data._1)
        var index = 0
        for(value <- newDataVector){
          pw.write(info._1+";"+data._1+";"+info._2+";"+info._3+";"+value+";"+getDateFromIndex(index) + "\n")
          index += 1
        }
      } else {
        val info = idMap(data._1)
        var index = 0
        for(value <- data._2){
          pw.write(info._1+";"+data._1+";"+info._2+";"+info._3+";"+value+";"+getDateFromIndex(index) + "\n")
          index += 1
        }
      }
      nr += 1
    }
    pw.close
  }


  def changeValue(value: Double): Double = {
    val round = Math.round(value)
    val extra = value-round
    if(extra > 0.05){
      return Math.ceil(value)
    } else if(extra < -0.05){
      return Math.floor(value)
    } else {
      return round
    }
  }

  def getCounts(countList: Array[String]): DenseVector[Int] =
  {
    val denseVector = DenseVector.zeros[Int](7200)
    var i = 0
    for(count <- countList)
    {
      if(i != 0)
      {
        denseVector(i-1) = count.toInt
      }
      i += 1
    }
    return denseVector
  }

  def correlation(countList1: DenseVector[Int], countList2: DenseVector[Int] ) : (Double, Double) =
  {
    var cor: Double = 0.0
    var x: Double = 0.0
    var sumx2: Double =0.0
    var y: Double = 0.0
    var sumy2: Double = 0.0
    var xy: Double = 0.0
    val usedValues = DenseVector.tabulate[Boolean](countList1.length)(id => true)

    var maxCount1 = 0
    var maxCount2 = 0
    var i = 0
    for(count <- countList1){
      if(count < 21){
        usedValues(i) = false
      }
      if(count > maxCount1){
        maxCount1 = count
      }
      i += 1
    }
    i = 0
    for(count <- countList2){
      if(count < 21){
        usedValues(i) = false
      }
      if(count > maxCount2){
        maxCount2 = count
      }
      i += 1
    }

    val rankList1 = DenseVector.zeros[Double](maxCount1-20)
    if(maxCount2 < 20){
      maxCount2 = 20
    }
    val rankList2 = DenseVector.zeros[Double](maxCount2-20)
    i = 0
    var length = 0
    for(used <- usedValues) {
      if (used) {
        length += 1
        rankList1(countList1(i)-21) += 1
        rankList2(countList2(i)-21) += 1
      }
      i += 1
    }
    if(length < 1000){
      return (-1, -1)
    }

    i = 0
    var sum = 0.0
    for(rank <- rankList1){
      val rank_value = rank
      rankList1(i) = sum + rank_value / 2
      sum += rank_value
      i += 1
    }
    i = 0
    sum = 0
    for(rank <- rankList2){
      val rank_value = rank
      rankList2(i) = sum + rank_value / 2
      sum += rank_value
      i += 1
    }

    i = 0
    var difference = 0.0
    for(used <- usedValues){
      if(used){
        length += 1
        x += rankList1(countList1(i)-21)
        sumx2 += Math.pow(rankList1(countList1(i)-21), 2)
        y += rankList2(countList2(i)-21)
        sumy2 += Math.pow(rankList2(countList2(i)-21), 2)
        xy += rankList1(countList1(i)-21) * rankList2(countList2(i)-21)
        difference += countList1(i) / countList2(i)
      }
      i += 1
    }

    val x2: Double = math.pow(x, 2)
    val y2: Double = math.pow(y, 2)

    val upper = (length * xy) - (x*y)
    val under = math.sqrt(((length * sumx2) - x2) * ((length * sumy2) - y2))
    cor = upper/under

    val avg_difference = difference / length

    return (cor, avg_difference)

  }

  def findSubs(countList1: DenseVector[Int], countList2: DenseVector[Int]) : (DenseVector[Double]) =
  {
    val usedValues = DenseVector.tabulate[Boolean](countList1.length)(id => true)
    val subValues = DenseVector.zeros[Double](countList1.length)

    var i = 0
    for(count <- countList1){
      if(count < 21){
        usedValues(i) = false
      }
      i += 1
    }

    i = 0
    for(used <- usedValues){
      if(!used){
        if(countList2(i) > 20){
          subValues(i) = countList2(i)
        }
      } else {
        if(i+1 < usedValues.length && !usedValues(i+1)){
          if(countList2(i) > 20){
            subValues(i) = countList2(i)
          }
        }
        if(i > 0 && !usedValues(i-1)){
          if(countList2(i) > 20){
            subValues(i) = countList2(i)
          }
        }
      }
      i += 1
    }
    return subValues
  }

  def findTensAndZeroes(ids: String, countList: DenseVector[Int]) : (String) =
  {
    var index = 0
    var countZero = 0
    var countTen = 0
    for(e <- countList)
    {
      if(e == 0)
      {
        countZero += 1
      }

      if(e == 10)
      {
        countTen += 1
      }
    }

    var sum = countTen + countZero

    println(countZero)

    //println("id: " + ids + " zeros: " + countZero + " Tens: " + countTen + " sum: " + sum)

    return ids
  }

  def getDateFromIndex(index: Int) : (String) =
  {
    val day = Math.floor(index/(60*24)).toInt
    val hour = Math.floor((index - day * (60*24))/60).toInt
    val min = Math.floor(index - day * (60*24) - hour * 60).toInt
    var hourString: String = ""+hour
    if(hour < 10){
      hourString = "0"+hour
    }
    var minString: String = ""+min
    if(min < 10){
      minString = "0"+min
    }
    return "2017-09-"+(25+day)+" "+hourString+":"+minString+":00"
  }

  def removeZeros(countList: DenseVector[Int]) : DenseVector[Int] =
  {
    val denseVector = DenseVector.zeros[Int](7200)

    var zeroIndexArray = Array(30, 121, 122, 976, 1561, 1562, 1661, 2407, 2408, 2783, 3001, 3002, 3913, 4441, 4442, 5355, 5881, 5882, 5883, 6190, 6792, 7050, -1, -1)
    var counter = 0

    for(e <- countList) {
      if (denseVector(counter) == 0) {
        denseVector(counter) = e

        for ((f, i) <- zeroIndexArray.zipWithIndex) {

          if (counter == f){
            if (zeroIndexArray(i + 1) == f + 1 && zeroIndexArray(i + 2) == f + 2) {
              if(countList(f+3) == 10)
              {
                denseVector(f) = 10
                denseVector(f + 1) = 10
                denseVector(f + 2) = 10
                denseVector(f + 3) = 10
              }
              else
              {
                denseVector(f) = countList(f + 3) / 4
                denseVector(f + 1) = countList(f + 3) / 4
                denseVector(f + 2) = countList(f + 3) / 4
                denseVector(f + 3) = countList(f + 3) / 4
              }
            }
            else if (zeroIndexArray(i + 1) == f + 1) {
              if(countList(f+2) == 10)
              {
                denseVector(f) = 10
                denseVector(f + 1) = 10
                denseVector(f + 2) = 10
              }
              else
              {
                denseVector(f) = countList(f + 2) / 3
                denseVector(f + 1) = countList(f + 2) / 3
                denseVector(f + 2) = countList(f + 2) / 3
              }

            }
            else //if(zeroIndexArray(i + 1) == f && zeroIndexArray(i + 1) != f + 1)
            {
              if(countList(f+1) == 10)
              {
                denseVector(f) = 10
                denseVector(f + 1) = 10
              }
              else
              {
                denseVector(f) = countList(f + 1) / 2
                denseVector(f + 1) = countList(f + 1) / 2
              }

            }
          }
        }
      }
      counter += 1
    }

    return denseVector

  }

  def makeTenDenseVector(countList: DenseVector[Int]) : DenseVector[Int] =
  {
    val tenDenseVector = DenseVector.zeros[Int](7200)
    var counter = 0
    var tenCounter = 0
    var maxCount = 0
    var containsTen = 0

    for(e<-countList)
    {
      if(e == 10)
      {
        containsTen = 1
        tenCounter += 1
      }
      else
      {
        tenCounter = 0
      }

      if(tenCounter > maxCount)
      {
        maxCount = tenCounter
      }
    }

    if(containsTen == 1)
    {
      if(maxCount < 10 && maxCount != 0)
      {
        for(e<- countList)
        {
          tenDenseVector(counter) = e
          counter += 1
        }
      }
      else
      {
        return null
      }
    }
    else
    {
      return null
    }

    return  tenDenseVector
  }

  def containKtens(countList: DenseVector[Int], k: Double): Boolean = {
    val allTens = countList.findAll(count => count < 21)
    if(allTens.isEmpty){
      return false
    }
    return allTens.length < countList.length*k
  }

}
