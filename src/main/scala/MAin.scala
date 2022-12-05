import io.netty.handler.codec.smtp.SmtpRequests.data
import org.apache.spark
import org.apache.spark.sql.functions.{col, hash}
import org.apache.spark.sql.types.{ByteType, DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scopt.OParser



//case class Config(
//                   argA: Int = -1,
//                   argB: Int = -1,
//                   debug: Boolean = false,
//                   string: String = "",
//                   list: Seq[String] = Seq(),
//                   map: Map[String, String] = Map(),
//                   command1: String = "",
//                   cmdArg: Int = 0
//                 )

object Main {
def main(args: Array[String]): Unit = {
//    OParser.parse(argParser, args, Config()) match {
//      case Some(config) =>
        val sparkSession = SparkSession.builder().master("local").getOrCreate() //vérifie si la sparksession existe
        val schema =
          StructType(
            Seq(
              StructField("id", IntegerType),
              StructField("nom", StringType),
              StructField("prenom", StringType),
              StructField("adresse", StringType),
              StructField("date", DateType))
          )

        val data: DataFrame = sparkSession.read.option("header", true).csv("data")
        val ft = data.filter("id=12")
        //data.printSchema()

        hashUP()
        deletebyId(13)


        def hashUP(): Unit = {
          //val up = data.withColumn("hash",hash(data.col("nom"),data.col("prenom"),data.col("adresse"))).show()
          val nomup = data.withColumn("nohash", hash(data.col("nom")))
          val prenomup = data.withColumn("phash", hash(data.col("prenom")))
          val adresseup = data.withColumn("ahash", hash(data.col("adresse")))

          val trans11 = nomup.withColumnRenamed("id", "idn")
          val trans12 = trans11.withColumnRenamed("date", "daten")
          val trans1 = data.join(trans12, data("id") === trans12("idn"), "inner")

          val trans21 = prenomup.withColumnRenamed("id", "idp")
          val trans22 = trans21.withColumnRenamed("date", "datep")
          val trans2 = trans1.join(trans22, data("id") === trans22("idp"), "inner")


          val trans31 = adresseup.withColumnRenamed("id", "ida")
          val trans32 = trans31.withColumnRenamed("date", "datea")
          val trans3 = trans2.join(trans32, data("id") === trans32("ida"), "inner")

          val trans4 = trans3.select("id", "nhash", "phash", "ahash", "date")
          trans4.show()
          trans4.write
            .mode(SaveMode.Overwrite)
            .csv("hash_fin")

        }

        def deletebyId(m: Int) = {
          val del = data.withColumn("nom", col("nom")).filter("id!=" + 13)
          del.show()
          del.write
            .mode(SaveMode.Overwrite)
            .csv("data_fin")
        }
//      case _ =>
//        exit(1)

    //}
  }
}




