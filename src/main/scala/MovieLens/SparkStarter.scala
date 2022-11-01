package MovieLens

import MovieLens.DataClean.cleanData
import MovieLens.DataSort.tokenize
import MovieLens.GenerateKeyValue.keyValueGenerator
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkStarter {

  def run(spark: SparkSession, inputFilePath: String, outputFilePath: String): Unit = {
    import spark.implicits._

    val data: Dataset[String] = spark.read.textFile(inputFilePath)

    val words: Dataset[String] = data
      .map(cleanData)
      .flatMap(tokenize)
      .filter(_.nonEmpty)

    val wordFrequencies: DataFrame = words
      .map(keyValueGenerator)
      .rdd.reduceByKey(_ + _)
      .toDF("word", "frequency")

    wordFrequencies.show(10)
    wordFrequencies.write.option("header","true").csv(outputFilePath)
    //    log.info(s"Result successfully written to $outputFilePath")
  }

}
