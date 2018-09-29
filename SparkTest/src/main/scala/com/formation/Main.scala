package com.formation

import com.formation.Driver.DriverSave
import com.formation.DriverNameTool.DriverName
import com.formation.DriverTimeSheet.Driver
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


//lit fichier timesheet conducteur
// fait la somme des heures et des miles pour chaque conducteur
// sauvegarder les résulats

object Main extends App
{
  val conf = new SparkConf().setAppName("drivers-timesheet").setMaster("local[*]").set("spark.executor.instances","3")
  val sc = new SparkContext(conf)

  val file = getClass.getClassLoader.getResource("timesheet.csv")
  val fileDriver = getClass.getClassLoader.getResource("drivers.csv")

  // read drivers.csv
  private val driverName: RDD[(String,DriverName)] = sc
    .textFile(fileDriver.getPath)
    .map(line => DriverNameTool.parseNameSimple(line))
    .map(driver => (driver.id, driver))

  // read timesheet.csv
  private val timesheet: RDD[(String,Driver)] = sc
    .textFile(file.getPath)
    .map(line => DriverTimeSheet.parseTimeSheet(line))
    .map(driver => (driver.id, driver))
    .reduceByKey(DriverTimeSheet.sumDriver)

  //join informations from both files
  val joinList = driverName
    .join(timesheet)
    .map{case (id, (driver, timesheet)) => DriverSave(driver.id,driver.name,timesheet.hours,timesheet.miles)}


  //format list with sort and convert to csv
  val listToCsv = joinList
      .sortBy(driver  => driver.id)
      .map(driver => Driver.toCsv(driver))
      .collect()

  //show list
  listToCsv
    .foreach(println)


  //save list
  val stringRdd = sc.parallelize(listToCsv)
  stringRdd.saveAsTextFile("out")



}

