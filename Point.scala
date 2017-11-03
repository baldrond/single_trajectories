package single_trajectories

/*
Class to define a point with longitude and latitude
 */
class Point(var lat: Double, var lng: Double){
  def toPrint(): String = {
    "Lat: "+lat+", Lng: "+lng
  }

  def equals(point: Point): Boolean = {
    if(point.lat == lat && point.lng == lng){
      return true
    }
    false
  }
}