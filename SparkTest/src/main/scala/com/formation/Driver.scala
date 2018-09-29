package com.formation

object Driver {

  //convert to csv
  def toCsv(driver: DriverSave): String = {

    val id : String  = driver.id
    val name :String  = driver.name
    val sumMiles :Int  = driver.sumMiles
    val sumHours: Int  = driver.sumHours

    s"$id,$name,$sumMiles,$sumHours"
  }


  case class DriverSave(id : String, name: String, sumHours: Int, sumMiles: Int)

}