package com.formation

import scala.util.{Failure, Success, Try}

object DriverNameTool {

  /// parse file driver.csv
  def parseName(line: String): Option[DriverName] = {
    Try {
      val lineValues: Array[String] = line.split(",")
      DriverName(
        id = lineValues(0),
        name = lineValues(1))
    } match {
      case Success(parseObject) => {
        Some(parseObject)
      }
      case Failure(_) => {
        None
      }
    }
  }

  def parseNameSimple(line: String): DriverName = {

      val lineValues: Array[String] = line.split(",")
      DriverName(
        id = lineValues(0),
        name = lineValues(1))
  }

  case class DriverName(id : String, name: String)

}

