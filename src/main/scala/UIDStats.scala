import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession 

object UIDStats{
	def main(args : Array[String]){
	val warehouseLoc = new File("spark-warehouse").getAbsolutePath 

// SparkSession is the starting point for loading Datasets/Dataframe from Spark2.X onwards
val spark = SparkSession.
           builder().
           appName("Spark Hive Example").
           config("spark.sql.warehouse.dir",warehouseLoc).
           enableHiveSupport().
           getOrCreate()

import spark.implicits._
import spark.sql


val uidDF = spark.read.
         format("csv").
         option("header", "true"). //reading the headers
         option("mode", "DROPMALFORMED").
         load("/Users/kiranrudresha/Documents/spark/uidproject/src/main/scala/data/")


uidDF.createOrReplaceTempView("uidtemp")

val statewise1 = sql("SELECT State, SUM(`Aadhaar generated`) as count FROM uidtemp GROUP BY State ORDER BY count DESC")

statewise1.show()

val agencywise1 = sql("SELECT `Enrolment Agency`, sUM(`Aadhaar generated`) as count FROM uidtemp GROUP BY `Enrolment Agency` ORDER BY count DESC")
	
agencywise1.show()

	}

}
