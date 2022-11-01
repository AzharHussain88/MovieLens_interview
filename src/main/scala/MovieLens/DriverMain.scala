package MovieLens

import MovieLens.SparkStarter.run
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Year
import org.apache.spark.sql.execution.command.ResetCommand.schemaString
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

object DriverMain {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Word Count").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val spark: SparkSession = SparkSession.builder().appName("WordCount").master("local[*]").getOrCreate()
    import spark.implicits._
    val sqlContext = spark.sqlContext

    run(spark, "D://Demo//data//MovieLens_Data//movies.item", "D://Demo//data//wordcount1")

    val movies1 = spark.read.format("csv").option("delimiter", "|").load("D://Demo//data//MovieLens_Data//movies.item")//.map(_.split("|"))

    import spark.implicits._

    val columnsdrop = List("_c3","_c5","_c6","_c7","_c8","_c9","_c10","_c11","_c12","_c13","_c14","_c15","_c16","_c17","_c18","_c19","_c20","_c21","_c22","_c23")

    val dropnullvalues = movies1.drop(columnsdrop:_*)

        val renamecol = dropnullvalues.withColumnRenamed("_c0","MovieID")
        .withColumnRenamed("_c1","Title")
        .withColumnRenamed("_c2","date")
        .withColumnRenamed("_c4","URL")

    val genresdf = spark.read.format("csv").option("header","true").load("D://Demo//data//MovieLens_Data//genres.csv")


    val dropcolgenre = genresdf.drop(col("title"))

    val renamedgenre = dropcolgenre.withColumnRenamed("movieId","MovieID")

    val dfjoin = renamecol.join(renamedgenre,renamecol("MovieID") === renamedgenre("MovieID"),"inner").drop(renamedgenre("MovieID"))

    val ratings1 = spark.read.format("csv").option("header","true").load("D://Demo//data//MovieLens_Data//ratings.csv")

    val ratingdf = ratings1.withColumnRenamed("movieId","MovieID").drop(col("timestamp"))

    val simpledf = dfjoin.join(ratingdf,dfjoin("MovieID") === ratingdf("MovieID"),"inner").drop(ratingdf("MovieID"))

    val perfectdf = simpledf.withColumn("Day", from_unixtime(unix_timestamp($"date", "dd-MMM-yyyy"), "dd"))
      .withColumn("Month", from_unixtime(unix_timestamp($"date", "dd-MMM-yyyy"), "MMMM"))
      .withColumn("Year", from_unixtime(unix_timestamp($"date", "dd-MMM-yyyy"), "YYYY")).drop(col("date"))

    perfectdf.createOrReplaceTempView("movie")
//    Top 10 Movie by Genre based on ratings
     spark.sql("SELECT m.title, m.rating,DENSE_RANK() OVER( ORDER BY rating DESC) as movie_rank FROM movie AS m  limit 10").show(10)

    //Most Popular and Least Popular movies within a 2 year span (Any Year)
    spark.sql("select MovieID,genres, Title, Year, rating from movie WHERE rating = '5.0' AND rating = '1.0' BETWEEN Year = '1990' AND Year = '2000' limit 10").show(10)

    //    Monthly Trend of ratings for The Top 2 Movies by Genre

    spark.sql("SELECT Month AS MONTH_NAME, COUNT(*) AS NUMBER_OF_MOVIES FROM  movie GROUP BY MONTH_NAME ORDER BY 2 Limit 2").show(10)





  }
}
