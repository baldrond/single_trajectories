package single_trajectories

/*
Class to define an edge between to points
Has a method to check if the edge collides with another edge:
  Checks if each point of an edge is on opposite side of the other edge and visa versa.
 */

class Edge(var p1: Point, var p2: Point){
  def collides(e: Edge): Boolean = {
    val t11 = (p1.lng - p2.lng)*(e.p1.lat - p1.lat) - (p1.lat - p2.lat)*(e.p1.lng - p1.lng)
    val t12 = (p1.lng - p2.lng)*(e.p2.lat - p1.lat) - (p1.lat - p2.lat)*(e.p2.lng - p1.lng)
    val t21 = (e.p1.lng - e.p2.lng)*(p1.lat - e.p1.lat) - (e.p1.lat - e.p2.lat)*(p1.lng - e.p1.lng)
    val t22 = (e.p1.lng - e.p2.lng)*(p2.lat - e.p1.lat) - (e.p1.lat - e.p2.lat)*(p2.lng - e.p1.lng)

    if((t11 >= 0  && t12 < 0) || (t12 >= 0  && t11 < 0)){
      if((t21 >= 0  && t22 < 0) || (t22 >= 0  && t21 < 0)){
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  def distBetween(): Double = {
    val earthRadius = 6371000.0
    val dLat = Math.toRadians(p2.lat-p1.lat)
    val dLng = Math.toRadians(p2.lng-p1.lng)
    val a = Math.sin(dLat/2) * Math.sin(dLat/2) +
      Math.cos(Math.toRadians(p1.lat)) * Math.cos(Math.toRadians(p2.lat)) *
        Math.sin(dLng/2) * Math.sin(dLng/2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a))
    earthRadius * c
  }

  def angleBetween(): Double = {
    val longDiff = Math.toRadians(p2.lng-p1.lng)
    val y = Math.sin(longDiff)*Math.cos(Math.toRadians(p2.lat))
    val x = Math.cos(Math.toRadians(p1.lat))*Math.sin(Math.toRadians(p2.lat)) -
      Math.sin(Math.toRadians(p1.lat))*Math.cos(Math.toRadians(p2.lat))*Math.cos(longDiff)

    ( Math.toDegrees(Math.atan2(y, x)) + 360 ) % 360
  }

  def equalsOpposite(edge: Edge): Boolean = {
    if(edge.p1.equals(p2) && edge.p2.equals(p1)){
      return true
    }
    false
  }

  def toPrint(): String = {
    "From: "+p1.toPrint()+" to: "+p2.toPrint()
  }
}
