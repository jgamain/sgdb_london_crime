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

    //crimes2016.orderBy((desc("totalValue"))).show(false)


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


    

    // *****************   category and avg per year *************** //


    val crimeByCategoryByBoroughByYear = londonCrimes
      .groupBy("year","borough","major_category")
      .sum("value")
      .withColumnRenamed("sum(value)", "total")
      .select("year","borough", "major_category","total")

    //crimeByCategoryByBoroughByYear.orderBy((desc("borough")),(desc(("total")))).show(false)

    val crimeByCategoryByBoroughAvgPerYear = londonCrimes
      .groupBy("year","borough","major_category")
      .sum("value")
      .groupBy("borough","major_category")
      .avg("sum(value)")
      .withColumnRenamed("avg(sum(value))", "averagePerYear")
      .select("borough", "major_category","averagePerYear")

    //crimeByCategoryByBoroughAvgPerYear.orderBy((desc("borough")),(desc(("averagePerYear")))).show(false)


    val crimeByMinorCategoryAvgPerYear = londonCrimes
      .groupBy("year","minor_category")
      .sum("value")
      .groupBy("minor_category")
      .avg("sum(value)")
      .withColumnRenamed("avg(sum(value))", "averagePerYear")
      .select("minor_category","averagePerYear")

    //crimeByMinorCategoryAvgPerYear.orderBy(desc("averagePerYear")).show(false)



    // *****************   diff_2008_2016_by_borough *************** //

    val nbCrimeByBorough2008 = londonCrimes
      .filter($"year" === "2008")
      .groupBy("borough").sum("value")
      .withColumnRenamed("sum(value)", "nbCrime2008")
      .select("borough","nbCrime2008")

    val nbCrimeByBorough2016 = londonCrimes
      .filter($"year" === "2016")
      .groupBy("borough").sum("value")
      .withColumnRenamed("sum(value)", "nbCrime2016")
      .select("borough","nbCrime2016")


    val df_2016 = nbCrimeByBorough2016.as("df2016")
    val df_2008 = nbCrimeByBorough2008.as("df2008")


    val join2008_2016 = df_2016
      .join(df_2008, col("df2016.borough") === col("df2008.borough"), "inner")
      .select("df2016.borough", "df2008.nbCrime2008", "df2016.nbCrime2016")

    //join2008_2016.orderBy(asc("borough")).show(false)


    val diff_2008_2016_by_borough = join2008_2016
      .withColumn("diff", col("df2016.nbCrime2016") - col("df2008.nbCrime2008"))
      .select("borough", "nbCrime2008", "nbCrime2016", "diff")

    diff_2008_2016_by_borough.orderBy(asc("borough")).show(false)


  }
}