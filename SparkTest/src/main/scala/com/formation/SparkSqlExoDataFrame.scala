package com.formation

import com.formation.SparkSqlTest.{getClass, spark}
import org.apache.spark.Success
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}


//charger les fichiers scans.csv et stores.csv
//enrichir les scnas avec les détails des magasions : town, name,...
//calculer le chiffre d'affaire global
//calculer le chiffre d'affaire par type de magasion Hyper/super
//calculer le produit avec la quantité la plus elever

object SparkSqlExoDataFrame extends  App
{
  val spark = SparkSession
    .builder()
    .appName("First Spark Sql example")
    .master("local[*]")
    .getOrCreate()


val scanSchema = StructType(Array(
  StructField("scanId",StringType,true),
  StructField("productName",StringType,true),
  StructField("storeId",StringType,true),
  StructField("sector",StringType,true),
  StructField("quantity",LongType,true),
  StructField("totalPrice",DoubleType,true)
))


val scansFile = getClass.getClassLoader.getResource("scans.csv")
val scans = spark
  .read
  .schema(scanSchema)
  .option("header",true)
  .csv(scansFile.getPath)

  val storesFile = getClass.getClassLoader.getResource("stores.csv")
val stores = spark
  .read
  .option("header",true)
  .csv(storesFile.getPath)

val joined = stores
 .join(scans,"storeId")


  import spark.implicits._

  joined.agg(sum("totalPrice")).show()
  joined.groupBy("typeStore").agg(sum("totalPrice")).show()

  val productname = joined
    .groupBy("productName")
    .agg(sum("quantity"))
    .sort(desc("sum(quantity)"))
    .limit(1)
    .select("productName")
    .show()


}
