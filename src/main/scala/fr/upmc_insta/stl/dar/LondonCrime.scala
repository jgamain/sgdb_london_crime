package fr.upmc_insta.stl.dar

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._


object LondonCrime {

  def readCsv(sparkSession: SparkSession, path: String): DataFrame = {
    sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }

  def main (arg: Array[String]): Unit = {
    println("Hello !");

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("LondonCrime")
      .getOrCreate()
    import sparkSession.implicits._

    val dataFolder = "C:\\Users\\Jeanne\\Documents\\Data\\london_crime\\"

    val londonCrimes = readCsv(sparkSession, dataFolder + "london_crime_by_lsoa.csv")

    val crimes2016 = londonCrimes
      .filter($"year" === "2016")
      //.filter($"value" =!= "0")
      .groupBy("major_category").sum("value")
      .withColumnRenamed("sum(value)", "totalValue")
      .select("major_category","totalValue")

    crimes2016.orderBy((desc("totalValue"))).show(false)


    val crimeByYearByCategory = londonCrimes
      .groupBy("year","major_category")
      .sum("value")
      .withColumnRenamed("sum(value)", "totalValue")
      .select("year", "major_category","totalValue")


    //crimeByYearByCategory.orderBy((desc("major_category")),(desc("year"))).show


    val totalCrimeByBoroughByYear = londonCrimes
      .groupBy("year","borough")
      .sum("value")
      .withColumnRenamed("sum(value)", "totalValue")
      .select("year", "borough","totalValue")

    //totalCrimeByBoroughByYear.orderBy((desc("borough")),(desc("year"))).show

  }
}