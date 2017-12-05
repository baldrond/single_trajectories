package single_trajectories

class Matrix_entry (var i: Int, var j: Int, var value: Double) extends Serializable {
  def addToValue(highest: Double): Unit = {
    if(highest != 0.0) {
      value += (1.0 - highest)
    }
    if(value != 1.0 && value != 2.0 && value > 0.9999){
      value = 1.0
    }
  }
  def subtractFromValue(highest: Double): Unit = {
    if(highest != 0.0) {
      value -= (1.0 - highest)
    }
  }

  def setValueZero(): Unit = {
    value = 0.0
  }

  def setValue(newValue: Double): Unit = {
    value = newValue
  }

  override def toString: String = {
    return "("+i+","+j+"): "+value
  }

  override def equals(obj: scala.Any): Boolean = {
    if(obj.isInstanceOf[Matrix_entry]){
      return obj.asInstanceOf[Matrix_entry].j == j && obj.asInstanceOf[Matrix_entry].i == this.i
    }
    return false
  }
}