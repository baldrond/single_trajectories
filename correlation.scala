package single_trajectories

import breeze.linalg.DenseVector
import org.apache.spark.{SparkConf, SparkContext}

object correlation {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Stavanger").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rawFile = sc.textFile("/Users/guro/Dokumenter/Skole/NTNU/Fordypningsprosjekt/Datasett/below20vector.csv")
    val rawRDD = rawFile.map(line => line.split(";"))

    val idAndCount = rawRDD.map(row=> (row(0), getCounts(row))).collect()


    val noZeroData = rawRDD.map(row=> (row(0), removeZeros(getCounts(row)))).collect()

    val tenData = noZeroData.map(row=> (row._1, makeTenDenseVector(row._2))).filter(row => row._2 != null)


    var cor = 0.0
    var maxCor = 0.0
    var id2 = ""
    for (e<-tenData)
    {
      for(f<- noZeroData)
      {
        if(e._1 != f._1)
        {
          cor = correlation(e._2, f._2)

          if(cor > maxCor)
          {
            id2 = f._1
            maxCor = cor
          }
        }
      }
      println("id1: " + e._1 + "  id2: " + id2 + " Max cor: " + maxCor)
      maxCor = 0.0
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

  def correlation(countList1: DenseVector[Int], countList2: DenseVector[Int] ) : (Double) =
  {
    var cor = 0.0
    var x = 0.0
    var sumx2 =0.0
    var y = 0.0
    var sumy2 =0.0
    var xy = 0.0

    for(count <- countList1)
    {
      x+=count
      sumx2 += Math.pow(count, 2)
    }

    var x2 = math.pow(x, 2)

    for(count <- countList2)
    {
      y+=count
      sumy2 += Math.pow(count, 2)
    }

    var y2 = math.pow(y, 2)

    for(i<-0 until countList1.length )
    {
      xy += countList1(i)*countList2(i)
    }

    val upper = (countList1.length * xy) - (x*y)
    val under = math.sqrt(((countList1.length * sumx2) - x2) * ((countList1.length * sumy2) - y2))
    cor = upper.toDouble/under.toDouble

    return cor

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
    // val index = (day - 25)*(60*24)+(min*60)+sec
    var date = ""
    var day = 0
    var hour = 0
    var min = 0

    if(index/(60*24) < 1)
    {
      day = 25
      if(index/60 < 0)
      {
        min = index
      }
      else
      {
        hour = index/60
        min = index - hour*60
      }
    }
    else if(index/(60*24) < 2)
    {
      day = 26
      val newIndex = index - (day-25)*(60*24)
      if(newIndex/60 < 0)
      {
        min = newIndex
      }
      else
      {
        hour = newIndex/60
        min = newIndex - hour*60
      }
    }
    else if(index/(60*24) < 3)
    {
      day = 27
      val newIndex = index - (day-25)*(60*24)
      if(newIndex/60 < 0)
      {
        min = newIndex
      }
      else
      {
        hour = newIndex/60
        min = newIndex - hour*60
      }
    }
    else if(index/(60*24) < 4)
    {
      day = 28
      val newIndex = index - (day-25)*(60*24)
      if(newIndex/60 < 0)
      {
        min = newIndex
      }
      else
      {
        hour = newIndex/60
        min = newIndex - hour*60
      }
    }
    else
    {
      day = 29
      val newIndex = index - (day-25)*(60*24)
      if(newIndex/60 < 0)
      {
        min = newIndex
      }
      else
      {
        hour = newIndex/60
        min = newIndex - hour*60
      }
    }

    date = (day + " " + hour + ":" + min)

    return date
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

}
