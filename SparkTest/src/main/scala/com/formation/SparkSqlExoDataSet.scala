package com.formation

import com.formation.SparkSqlExoDataFrame.spark
import com.formation.SparkSqlTest.etudiants
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


//charger les fichiers scans.csv et stores.csv
//enrichir les scnas avec les détails des magasions : town, name,...
//calculer le chiffre d'affaire global
//calculer le chiffre d'affaire par type de magasion Hyper/super
//calculer le produit avec la quantité la plus elever
object SparkSqlExoDataSet extends  App
{


  val spark = SparkSession
    .builder()
    .appName("First Spark Sql example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

val scanSchema = StructType(Array(
  StructField("scanId",StringType,true),
  StructField("productName",StringType,true),
  StructField("storeId",StringType,true),
  StructField("sector",StringType,true),
  StructField("quantity",IntegerType,true),
  StructField("totalPrice",FloatType,true)
))


val scansFile = getClass.getClassLoader.getResource("scans.csv")
val scans = spark
  .read
  .schema(scanSchema)
  .option("header",true)
  .csv(scansFile.getPath)
    .as[Scan]

  val storeSchema = StructType(Array(
    StructField("storeId",StringType,true),
    StructField("postCode",StringType,true),
    StructField("geoLocation",StringType,true),
    StructField("name",StringType,true),
    StructField("town",StringType,true),
    StructField("typeStore",StringType,true)
  ))


  val storesFile = getClass.getClassLoader.getResource("stores.csv")
val stores = spark
  .read
  .schema(storeSchema)
  .option("header",true)
  .csv(storesFile.getPath)
  .as[Store]

  val joined = scans
    .join(stores, "storeId")
    .as[ScanComplex]

  joined.agg(sum("totalPrice")).show()
  joined.groupBy("typeStore").agg(sum("totalPrice")).show()

  val productname = joined
    .groupBy("productName")
    .agg(sum("quantity"))
    .sort(desc("sum(quantity)"))
    .limit(1)
    .select("productName")


  productname.write.json("data/product_json")


case class Store(storeId:String,postCode:String,geoLocation:String,name:String,town:String, typeStore:String)
case class Scan(scanId:String,productName:String,storeId:String,sector:String,quantity:Int,totalPrice:Float)
case class ScanComplex(scanId:String,productName:String,storeId:String,sector:String,quantity:Int,totalPrice:Float,name:String,town:String)

}
