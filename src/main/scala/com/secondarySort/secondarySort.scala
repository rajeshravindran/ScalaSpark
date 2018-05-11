import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class NamesSchema(lastName:String, firstName:String)

val namesSchema = StructType(
  StructField("LASTNAME", StringType, true)::
  StructField("FIRSTNAME",StringType,true)::Nil
)

object secondarySort{
  val spark = SparkSession.builder().appName("Secondary Sort Sample").master("local[*]").enableHiveSupport().getOrCreate()

  val filePath = "/Users/greeshma/ScalaSpark/data/Names.csv"

  import spark.implicits._

  def main(args:Array): Unit ={

    /*
    * METHOD 1 USE ORDERBY METHOD OF DATAFRAME
    * */
    val namesDF = spark.read.format("csv").schema(namesSchema).load(filePath).as[NamesSchema]
    val orderedDF = namesDF.orderBy("LASTNAME","FIRSTNAME")
    orderedDF.show(10)

    /*
    * METHOD 2 USING RDD
    * */

    val conf = new SparkConf().setAppName("SECONDARY SORT EXAMPLE")
    val sc = new SparkContext(conf)

    val namesRDD = sc.textFile(filePath).map(_.split(",")).map{k=>(k(0),k(1))}
    val grpBy = namesRDD.groupByKey(4).mapValues(iter => iter.toList.sortBy(r => r))

    val resultRDD = grpBy.flatMap{
      case (key, listValues) =>{
        listValues.map((key,_))
      }
    }.toDF("LASTNAME", "FIRSTNAME")



  }
}