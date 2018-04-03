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

  def exportCsv(df: DataFrame, fileName: String)={
    df
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header",true)
      .save(fileName);
  }

  def main (arg: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("LondonCrime")
      .getOrCreate()
    import sparkSession.implicits._

    val dataFolder = "./"

    val londonCrimes = readCsv(sparkSession, dataFolder + "london_crime_by_lsoa.csv").as("c")


    // ****************** DANS QUELS QUARTIERS LA CRIMINALITÉ EST-ELLE LA PLUS ÉLEVÉE ? ***************** //

    val crimeByBorough = londonCrimes
      .groupBy("borough","year")
      .sum("value")
      .groupBy("borough")
      .avg("sum(value)")
      .withColumnRenamed("avg(sum(value))","avgCrimesPerYear")
      .withColumn("avgCrimesPerYear", $"avgCrimesPerYear".cast(IntegerType))
      .select("borough", "avgCrimesPerYear").as("cb")

    //val crimeByBoroughOrderByTotal = crimeByBorough.orderBy((desc("avgCrimesPerYear")))

    //crimeByBoroughOrderByTotal.show(false)

    /*EXPORT CSV*/
    exportCsv(crimeByBorough, "crimeByBorough.csv");


    //Taux de criminalité (en pourcentage)
    val lsoaData = readCsv(sparkSession, dataFolder + "lsoa-data_iadatasheet1.csv").as("lsoa")

    val populationByBorough = londonCrimes
      .groupBy("lsoa_code", "borough")
      .count()
      .select("lsoa_code", "borough")
      .join(lsoaData, $"lsoa.Codes"===$"c.lsoa_code")
      .groupBy("borough")
      .sum("2011")
      .withColumnRenamed("sum(2011)", "population")
      .select("borough", "population").as("pb")

    //populationByBorough.show(30, false)

    val criminality = crimeByBorough
      .join(populationByBorough, $"cb.borough"===$"pb.borough")
      .withColumn("rate", (col("cb.avgCrimesPerYear") / col("pb.population"))*100)
      .withColumn("rate", $"rate".cast(IntegerType))
      .select("cb.borough", "avgCrimesPerYear", "population", "rate")

    //criminality.show(30, false)

    /*EXPORT CSV*/
    exportCsv(criminality, "criminality.csv");


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

    /*EXPORT CSV*/
    exportCsv(crimeByMajorCategoryAvg, "crimeByMajorCategoryAvg.csv");


    val crimeByMinorCategoryAvg = londonCrimes
      .groupBy("year","minor_category","major_category")
      .sum("value")
      .groupBy("minor_category","major_category")
      .avg("sum(value)")
      .withColumnRenamed("avg(sum(value))", "averagePerYear")
      .withColumn("averagePerYear", $"averagePerYear".cast(IntegerType))
      .select("major_category","minor_category","averagePerYear")

    //crimeByMinorCategoryAvg.orderBy(desc("averagePerYear")).show(50, false)

    /*EXPORT CSV*/
    exportCsv(crimeByMinorCategoryAvg, "crimeByMinorCategoryAvg.csv");


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

    /*EXPORT CSV*/
    exportCsv(crimeWestminsterMajorAvg, "crimeWestminsterMajorAvg.csv");

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


    /*EXPORT CSV*/
    exportCsv(westminsterTheftsAvg, "westminsterTheftsAvg.csv");


    // ************ COMMENT ÉVOLUE LE NOMBRE DE CRIMES AU COURT DE L’ANNÉE ? ******************* //

    val months = readCsv(sparkSession, dataFolder + "months.csv").as("m")

    val crimeByMonth = londonCrimes
      .join(months, $"c.month" === $"m.month_id")
      .groupBy("c.year", "m.month_id","m.month_name")
      .sum("value")
      .withColumnRenamed("sum(value)", "total")
      .select("c.year", "m.month_id", "m.month_name", "total")

    //val crimeByMonthOrdered = crimeByMonth.orderBy(asc("c.year"), asc("m.month_id"))

    //crimeByMonthOrdered.show(36)

       val crimeByMonthAvg = crimeByMonth
      .groupBy("m.month_id","m.month_name")
      .avg("total")
      .withColumnRenamed("avg(total)", "average")
      .withColumn("average", $"average".cast(IntegerType))
      .select("m.month_id", "m.month_name", "average")

    val crimeByMonthAvgOrdered = crimeByMonthAvg.orderBy(asc("m.month_id"))

    /*EXPORT CSV*/
    exportCsv(crimeByMonthAvg, "crimeByMonthAvg.csv");

    //crimeByMonthAvgOrdered.show

  }
}