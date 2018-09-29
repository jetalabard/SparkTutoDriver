package com.formation

import com.formation.MainV2.getClass
import com.sun.org.apache.xalan.internal.xsltc.compiler.util.StringType
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}


import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType

object SparkSqlTest extends  App{

  val spark = SparkSession
    .builder()
    .appName("First Spark Sql example")
      .master("local[*]")
    .getOrCreate()


  import spark.implicits._

  val file = getClass.getClassLoader.getResource("etudiants1.json")

  val etudiants = spark.read.json(file.getPath)

  etudiants.show()
  etudiants.select("age").show()

  etudiants.filter($"age" > 22).show()
  etudiants.groupBy("departement").count().show()

  val file2 = getClass.getClassLoader.getResource("etudiants2.json")
  val etudiants2 = spark.read.json(file2.getPath)

  etudiants2.printSchema()
  etudiants.printSchema()
  //etudiants: DataFrame = [id:int, nom:string,age:int, departement:string]

  /*  root
    |-- age: long (nullable = true)
    |-- departement: string (nullable = true)
    |-- id: long (nullable = true)
    |-- nom: string (nullable = true)
  */

  etudiants.union(etudiants2).show()

  etudiants.groupBy("departement").count().show()

  /*etudiants.agg(sum("age").alias("age_sum")).show()
  etudiants.agg(min("age").alias("age_min")).show()
  etudiants.agg(max("age").alias("age_max")).show()
  etudiants.agg(avg("age").alias("age_avg")).show()*/

  etudiants.groupBy("departement").max("age").show()
  etudiants.groupBy("departement").min("age").show()


  SaveMode.ErrorIfExists
  SaveMode.Append
  SaveMode.Overwrite
  SaveMode.Ignore

  etudiants.write.json("data/etudiants_json")
  etudiants.write.csv("data/etudiants_csv")

  //import org.apache.spark.sql.SaveMode
  etudiants.write.mode(SaveMode.Overwrite).json("data/etudiants_json")
  etudiants.write.mode(SaveMode.Overwrite).csv("data/etudiants_csv")

  val file3 = getClass.getClassLoader.getResource("etudiants1.csv")
  val etudiants3 = spark.read.option("header","true").csv(file3.getPath)

  //etudiants3: DataFrame = [id:string, nom:string,age:string, departement:string]

  /*  root
    |-- age: string (nullable = true)
    |-- departement: string (nullable = true)
    |-- id: string (nullable = true)
    |-- nom: string (nullable = true)
  */

  etudiants3.groupBy("departement").sum("age").show() //Error car age n'est pas un entier

  /*val etudiantSchema = StructType(Array(
     StructField("id",LongType,true),
     StructField("nom",StringType,true),
     StructField("age",LongType,true),
     StructField("departement",StringType,true)))

  val etudiants4 = spark.read.schema(etudiantSchema).option("header",true).csv("etudiants1.csv")

  etudiants4.groupBy("departement").sum("age").show()// fonctionne
*/


  etudiants3.createOrReplaceTempView("etudiant")

  spark.sql("Select * from etudiant").show()

  etudiants3.createGlobalTempView("etudiant_glob") // global si on commence une nouvelle session on la voit encore

  spark.newSession().sql("Select * from etudiant_glob").show()

}
