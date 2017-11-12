package single_trajectories

class Matrix_entry (var i: Int, var j: Int, var value: Double) extends Serializable {
  def addToValue(add_value: Double): Unit = {
    value += add_value
  }
  override def toString: String = {
    return "("+i+","+j+"): "+value
  }
}