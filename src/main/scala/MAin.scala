import io.netty.handler.codec.smtp.SmtpRequests.data
import jdk.nashorn.internal.runtime.regexp.joni.constants.Arguments
import org.apache.spark
import org.apache.spark.sql.functions.{col, hash, map_values}
import org.apache.spark.sql.types.{ByteType, DateType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scopt.OParser
import modules.{Config, Deletefunct}
import modules.Deletefunct.{data, deletebyId}
import modules.HashFunct.{hashUP, sparkSession}

object Main {
  val builder = OParser.builder[Config]
  val parser1 = {
    import builder._
    OParser.sequence(
      programName("appli"),
  head("appli", "0.1"),
  opt[Long]('d',"delete")
    .optional()
    .action((value, argument) => argument.copy( delete= value))
    .text("deleting the data for the id specified"),
  opt[Long]('h',"hash")
    .optional()
    .action((value, argument) => argument.copy( hash= value))
    .text("deleting the data for the id specified"))
  }



def main(args: Array[String]): Unit = {
  OParser.parse(parser1, args, Config()) match {
    case Some(config) =>
      val id_del = config.delete
      val id_hash = config.hash
    if (id_del > -1){
      val data: DataFrame = sparkSession.read.option("header", true).csv("data")

      if (data.filter("id=" + id_del).isEmpty){
        print("mauvais id = "+ id_del)
      } else{
        print("del" +id_del)
        deletebyId(id_del)
      }
    } else if (id_hash > -1){
      val data: DataFrame = sparkSession.read.option("header", true).csv("data")

      if (data.filter("id=" + id_hash).isEmpty){
        print("mauvais id = "+ id_hash)
      } else{
        print("hash" + id_hash)
        hashUP(id_hash)


      }


    }
    case _ =>
    print("error")
  }

  }
}





