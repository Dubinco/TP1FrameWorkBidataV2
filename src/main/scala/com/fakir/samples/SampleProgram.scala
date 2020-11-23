package com.fakir.samples

import com.fakir.samples.config.ConfigParser
import com.fakir.samples.utils.SparkReaderWriter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object SampleProgram {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    //val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //val configCli = ConfigParser.getConfigArgs(args)

    //val df = SparkReaderWriter.readData(configCli.inputPath, configCli.inputformat)

    val configCli = ConfigParser.getConfigArgs(args)
    //println(configCli.partitions)
    val df = SparkReaderWriter.readData(configCli.inputPath, configCli.inputFormat, hasHeader = true)
    //transform etc!
    //val selectedDf = df.select("Region", "Country")
    val dfWithoutId = df.filter(col("ID") =!= configCli.id)
    dfWithoutId.show
    SparkReaderWriter.writeData(
      dfWithoutId, configCli.outputPath, configCli.outputFormat, overwrite = true)

  }
}
