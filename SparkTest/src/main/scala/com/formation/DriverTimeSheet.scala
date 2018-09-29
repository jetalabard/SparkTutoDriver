package com.formation

import org.apache.spark.util.LongAccumulator

import scala.util.{Failure, Success, Try}

object DriverTimeSheet {

  /// parse file timesheet.csv
  def parseTimeSheet(line: String, accumulatorError: LongAccumulator, accumulatorSuccess: LongAccumulator): Option[Driver] = {

    Try {
      val lineValues: Array[String] = line.split(",")
      Driver(
        id = lineValues(0),
        week = lineValues(1).toInt,
        hours = lineValues(2).toInt,
        miles = lineValues(3).toInt)
    } match {
      case Success(parseObject) => {
        accumulatorSuccess.add(1)
        Some(parseObject)
      }
      case Failure(_) => {
        accumulatorError.add(1)
        None
      }
    }
  }

  /// parse file timesheet.csv
  def parseTimeSheet(line: String): Driver = {

      val lineValues: Array[String] = line.split(",")
      Driver(
        id = lineValues(0),
        week = lineValues(1).toInt,
        hours = lineValues(2).toInt,
        miles = lineValues(3).toInt)

  }

  /// sum two drivers
  def sumDriver(driver1 :Driver, driver2: Driver) :Driver = {
    Driver(driver1.id,0,driver1.hours + driver2.hours, driver1.miles + driver2.miles)
  }

  case class Driver(id : String, week: Int, hours: Int, miles: Int)
}
