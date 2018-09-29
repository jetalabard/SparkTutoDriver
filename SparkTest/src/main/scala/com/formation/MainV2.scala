package com.formation

import com.formation.Driver.DriverSave
import com.formation.DriverNameTool.DriverName
import com.formation.DriverTimeSheet.Driver
import com.formation.MainV2.{accumulatorError, accumulatorSuccess}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import scala.util.{Failure, Success, Try}


object MainV2 extends App
{
  val conf = new SparkConf().setAppName("drivers-timesheet").setMaster("local[*]").set("spark.executor.instances","3")
  val sc = new SparkContext(conf)

  val file = getClass.getClassLoader.getResource("timesheetV2.csv")
  val fileDriver = getClass.getClassLoader.getResource("driversV2.csv")


  var accumulatorError = sc.longAccumulator("Error Count")
  var accumulatorSuccess = sc.longAccumulator("Success Count")

  // read drivers.csv
  private val driverName: Map[String, DriverName] = Source.fromFile(fileDriver.getPath).getLines()
    .flatMap(line => DriverNameTool.parseName(line))
    .map(driver => (driver.id, driver)).toMap

  // put driverName to broadcast
  val referentielName = sc.broadcast(driverName)

  // read timesheet.csv
  private val timesheet: RDD[(String,Driver)] = sc
    .textFile(file.getPath)
    .flatMap(line => DriverTimeSheet.parseTimeSheet(line,accumulatorError,accumulatorSuccess))
    .map(driver => (driver.id, driver))
    .reduceByKey(DriverTimeSheet.sumDriver)

  //join informations from both files
  val listToCsv = timesheet
    .map{case (id, driver) => {
      val name = referentielName.value(id).name
      DriverSave(driver.id,name,driver.hours,driver.miles)
    }}
    .sortBy(driver  => driver.id)
    .map(driver => Driver.toCsv(driver))
    .collect()


  //show list
  listToCsv
    .foreach(println)


  //save list
  val stringRdd = sc.parallelize(listToCsv)
  stringRdd.saveAsTextFile("out")

  println(s"Nb row success : ${accumulatorSuccess.value}")
  println(s"Nb row error : ${accumulatorError.value}")


  Thread.sleep(5000)

}
