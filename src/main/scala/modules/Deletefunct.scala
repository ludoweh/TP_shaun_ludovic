package modules


import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, LongType, StringType, StructField, StructType}

object Deletefunct {

  val sparkSession = SparkSession.builder().master("local").getOrCreate() //vérifie si la sparksession existe
  val schema =
    StructType(
      Seq(
        StructField("id", LongType),
        StructField("nom", StringType),
        StructField("prenom", StringType),
        StructField("adresse", StringType),
        StructField("date", DateType))
    )

  val data: DataFrame = sparkSession.read.option("header", true).csv("data")

  def deletebyId(m: Long) = {
    val del = data.withColumn("nom", col("nom")).filter("id!=" + m)
    del.show()
              del.write
                .mode(SaveMode.Overwrite)
                .csv("data_fin")
    val fin = data.filter("id!="+13).withColumn("nom", col("nom")).write.csv("data_fin")
  }
}
