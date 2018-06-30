import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

case class stringText(str:String)

object StrAndReverse extends App{

  val spark = SparkSession.builder().appName("Find the Words from List only if it has its reverse USING SPARKSQL").master("local[*]").enableHiveSupport().getOrCreate()

  val conf = new SparkConf().setAppName("Find the Words from List only if it has its reverse USING RDD").setMaster("local[*]")
  val sparkCtx = new SparkContext(conf)

  import spark.implicits._

  val fileName = "data/stringList.txt"
  
  def main(args: Array): Unit ={

    ///IMPLEMENTATION USING SPARK SQL

    val input = spark.read.textFile(fileName).map(x=>stringText(x))

    input.createTempView("STRTEXT")

    val filteredRecords = spark.sql("select A.str from STRTEXT A inner join STRTEXT B ON A.STR = REVERSE(B.STR)")

    filteredRecords.show(100)


    ///IMPLEMENTATION USING RDD

    val input1 = sparkCtx.textFile(fileName)

    val x = input1.map(x=>(x,1))

    val y = input1.map(x => (x.reverse,1))

    val z = x.join(y).map(t=>t._1)

    z.take(100)
  }


}