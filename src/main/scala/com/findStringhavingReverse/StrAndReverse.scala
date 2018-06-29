import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

case class stringText(str:String)

object StrAndReverse extends App{

  val spark = SparkSession.builder().appName("Find the Words from List only if it has its reverse ").master("local[*]").enableHiveSupport().getOrCreate()

  import spark.implicits._

  val fileName = "data/stringList.txt"
  
  def main(args: Array): Unit ={

    val input = spark.read.textFile(fileName).map(x=>stringText(x))

    input.createTempView("STRTEXT")

    val filteredRecords = spark.sql("select A.str from STRTEXT A inner join STRTEXT B ON A.STR = REVERSE(B.STR)")

    filteredRecords.show(100)
  }


}