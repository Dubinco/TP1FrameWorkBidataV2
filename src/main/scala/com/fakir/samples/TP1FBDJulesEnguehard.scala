package com.fakir.samples

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, udf, when}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object TP1FBDJulesEnguehard {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    """Exercice 1 - RDD: """
    println("EXERCICE 1")

    //1 Lire le fichier "films.csv" sous forme de RDD[String]
    val rdd = sparkSession.sparkContext.textFile("src/main/scala/com/fakir/samples/donnees.csv")
    println("Les films")
    rdd.foreach(println)

    //2 Combien y a-t-il de films de Leonardo Di Caprio dans ce fichier ?

    val DiCapriordd = rdd.filter(elem=> elem.split(";")(3) == "Di Caprio")
    println("Les films de Di Caprio")
    DiCapriordd.foreach(println)
    println("Nombre de films de Di Caprio")
    println(DiCapriordd.count())

    //3 Quelle est la moyenne des notes des films de Di Caprio ?
    val averageDiCaprio = DiCapriordd.map(_.split(";")).map(_(2).toDouble).mean()
    println("La moyenne des notes des films de Di Caprio")
    println(averageDiCaprio)

    //4 Quel est le pourcentage de vues des films de Di Caprio par rapport à l'échantillon que nous avons ?
    val sumDiCaprio = DiCapriordd.map(_.split(";")).map(_(1).toLong).sum()
    val sumTotal = rdd.map(_.split(";")).map(_(1).toLong).sum()
    val percentage = 100*(sumDiCaprio / sumTotal)
    println("Le pourcentage de vues des films de Di Caprio sur l'ensemble des films")
    println(percentage + " %")

    //5. Quelle est la moyenne des notes par acteur dans cet échantillon ?
    //    1. Pour cette question, il faut utiliser les Pair-RDD

    val pairRDD = rdd.keyBy(elem => elem.split(";")(3))
    val withValue = pairRDD.mapValues(e => (1.0,(e.split(";")(2)).toDouble))
    val countSums = withValue.reduceByKey((x,y) =>(x._1 + y._1, x._2 + y._2))
    val keyMeans = countSums.mapValues(avgCount1 => avgCount1._2 / avgCount1._1)
    println("Moyenne de note par acteur")
    keyMeans.foreach(println)

    //6 Quelle est la moyenne des vues par acteur dans cet échantillon ?

    val pairRDD2 = rdd.keyBy(elem => elem.split(";")(3))
    val withValue2 = pairRDD2.mapValues(e => (1.0,(e.split(";")(1)).toDouble))
    val countSums2 = withValue2.reduceByKey((x,y) =>(x._1 + y._1, x._2 + y._2))
    val keyMeans2 = countSums2.mapValues(avgCount1 => avgCount1._2 / avgCount1._1)
    println("Moyenne de vue par acteur")
    keyMeans2.foreach(println)

    """## Exercice 2 - DataFrames:"""
    println("EXERCICE 2")
    //1. Lire le fichier "films.csv" avec la commande suivante :
      //1. spark.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("films.csv")
      //val df2 = sparkSession.read.format("com.databricks.spark.csv").option("delimiter", ";").load("src/main/scala/com/fakir/samples/donnees.csv")
    val df2 = sparkSession.read.format("com.databricks.spark.csv").option("delimiter", ";").load("src/main/scala/com/fakir/samples/donnees.csv")
    //2. Nommez les colonnes comme suit : nom_film, nombre_vues, note_film, acteur_principal
    val df3 = df2.withColumnRenamed("_c0", "nom_film")
    val df4 = df3.withColumnRenamed("_c1", "nombre_vues")
    val df5 = df4.withColumnRenamed("_c2", "note_film")
    val df6 = df5.withColumnRenamed("_c3", "acteur_principal")
    //df6.show()
    val dftype = df6.withColumn("nom_film",col("nom_film").cast(StringType))
      .withColumn("nombre_vues",col("nombre_vues").cast(LongType))
      .withColumn("note_film",col("note_film").cast(DoubleType))
      .withColumn("acteur_principal",col("acteur_principal").cast(StringType))

    //3. Refaire les questions 2, 3, 4 et 5 en utilisant les DataFrames.
    //3.2 Combien y a-t-il de films de Leonardo Di Caprio dans ce fichier ?
    val DiCaprio = dftype.filter(col("acteur_principal") === "Di Caprio")
    println("Combien y a-t-il de films de Leonardo Di Caprio dans ce fichier ?")
    println(DiCaprio.count())

    //3.3 Quelle est la moyenne des notes des films de Di Caprio ?
    val DiCapriomean = DiCaprio.groupBy("acteur_principal").mean("note_film")
    println("Quelle est la moyenne des notes des films de Di Caprio ?")
    DiCapriomean.show()

    //3.4 Quel est le pourcentage de vues des films de Di Caprio par rapport à l'échantillon que nous avons ?
    val DiCaprioSum = DiCaprio.groupBy("acteur_principal").sum("nombre_vues").collect()
    println(DiCaprioSum)
    val total = dftype.groupBy().sum("nombre_vues").collect()
    println(total)
    //val DiCaprioPercentage = 100*(DiCaprioSum/total)
    println("Quel est le pourcentage de vues des films de Di Caprio par rapport à l'échantillon que nous avons ?")
    //println(DiCaprioPercentage+" %")


    //4. Créer une nouvelle colonne dans ce DataFrame, "pourcentage de vues", contenant le pourcentage de vues pour chaque film (combien de fois les films de cet acteur ont-ils été vus par rapport aux vues globales ?



    }

}
