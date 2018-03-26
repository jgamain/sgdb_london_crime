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

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("LondonCrime")
      .getOrCreate()
    import sparkSession.implicits._

    val dataFolder = "./"

    val londonCrimes = readCsv(sparkSession, dataFolder + "london_crime_by_lsoa.csv")

    /*
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
      .withColumn("averagePerYear", $"averagePerYear".cast(IntegerType))
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

    //diff_2008_2016_by_borough.orderBy(asc("borough")).show(false)

*/


    // ****************** DANS QUELS QUARTIERS LA CRIMINALITÉ EST-ELLE LA PLUS ÉLEVÉE ? ***************** //

    val crimeByBorough = londonCrimes
      .groupBy("borough","year")
      .sum("value")
      .groupBy("borough")
      .avg("sum(value)")
      .withColumnRenamed("avg(sum(value))","avgCrimesPerYear")
      .withColumn("avgCrimesPerYear", $"avgCrimesPerYear".cast(IntegerType))
      .select("borough", "avgCrimesPerYear")

    //val crimeByBoroughOrderByTotal = crimeByBorough.orderBy((desc("avgCrimesPerYear")))

    //crimeByBoroughOrderByTotal.show(false)


    // ****************** QUELS SONT LES CRIMES LES PLUS COURANTS ? ******************* //

    val crimeByMajorCategoryAvg = londonCrimes
      .groupBy("year","major_category")
      .sum("value")
      .groupBy("major_category")
      .avg("sum(value)")
      .withColumnRenamed("avg(sum(value))", "averagePerYear")
      .withColumn("averagePerYear", $"averagePerYear".cast(IntegerType))
      .select("major_category","averagePerYear")

    //crimeByMajorCategoryAvg.orderBy(desc("averagePerYear")).show(false)

    val crimeByMinorCategoryAvg = londonCrimes
      .groupBy("year","minor_category")
      .sum("value")
      .groupBy("minor_category")
      .avg("sum(value)")
      .withColumnRenamed("avg(sum(value))", "averagePerYear")
      .withColumn("averagePerYear", $"averagePerYear".cast(IntegerType))
      .select("minor_category","averagePerYear")

    //crimeByMinorCategoryAvg.orderBy(desc("averagePerYear")).show(50, false)


    // ************ QUELS SONT LES CRIMES LES PLUS COURANTS À WESTMINSTER? ******************* //

    val crimeWestminsterMajorAvg = londonCrimes
      .filter($"borough" === "Westminster")
      .groupBy("year","major_category")
      .sum("value")
      .groupBy("major_category")
      .avg("sum(value)")
      .withColumnRenamed("avg(sum(value))", "avgPerYear")
      .withColumn("avgPerYear", $"avgPerYear".cast(IntegerType))
      .select("major_category","avgPerYear")

    //crimeWestminsterMajorAvg.orderBy(desc("avgPerYear")).show(false)

    val westminsterTheftsAvg = londonCrimes
      .filter($"borough" === "Westminster")
      .filter($"major_category" === "Theft and Handling")
      .groupBy("year","minor_category")
      .sum("value")
      .groupBy("minor_category")
      .avg("sum(value)")
      .withColumnRenamed("avg(sum(value))", "avgPerYear")
      .withColumn("avgPerYear", $"avgPerYear".cast(IntegerType))
      .select("minor_category","avgPerYear")

    //westminsterTheftsAvg.orderBy(desc("avgPerYear")).show(false)



    // ************ COMMENT ÉVOLUE LE NOMBRE DE CRIMES AU COURT DE L’ANNÉE ? ******************* //

    val months = readCsv(sparkSession, dataFolder + "months.csv").as("m")

    val crimes = londonCrimes.as("c")

    val crimeByMonth = crimes
      .join(months, $"c.month" === $"m.month_id")
      .groupBy("c.year", "m.month_id","m.month_name")
      .sum("value")
      .withColumnRenamed("sum(value)", "total")
      .select("c.year", "m.month_id", "m.month_name", "total")

    //val crimeByMonthOrdered = crimeByMonth.orderBy(asc("c.year"), asc("m.month_id"))

    //crimeByMonthOrdered.show(36)


    val crimeByMonthAvg = crimes
      .join(months, $"c.month" === $"m.month_id")
      .groupBy("c.year", "m.month_id","m.month_name")
      .sum("value")
      .groupBy("m.month_id","m.month_name")
      .avg("sum(value)")
      .withColumnRenamed("avg(sum(value))", "average")
      .withColumn("average", $"average".cast(IntegerType))
      .select("m.month_id", "m.month_name", "average")

    val crimeByMonthAvgOrdered = crimeByMonthAvg.orderBy(asc("m.month_id"))

    crimeByMonthAvgOrdered.show

  }
}