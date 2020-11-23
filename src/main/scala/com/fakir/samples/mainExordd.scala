package com.fakir.samples

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, udf, when}
import org.apache.spark.sql.{SaveMode, SparkSession}

object mainExordd {

  def majuscule(s: String, filtre: String): String = {
    if (s.contains(filtre)) s
    else s.toUpperCase
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //session 1
    val rddnom = sparkSession.sparkContext.textFile("data/dataOct-22-2020.csv")
    rddnom.foreach(println)
    """on récupère que les users qui ne commence pas par F et qui ont plus que 2 enfants"""
    val suprdd = rddnom.filter(elem => !elem.contains("F") | elem.split(";")(1).toDouble > 2)
    //suprdd.foreach(println)
    //println(suprdd.count())

    """on fait la moyenne du nombre d'enfant par age en faisant des keys sur l'age"""
    //val counts = suprdd.map(item => (item.split(";")(2).toDouble, (1.0, item.split(";")(1).toDouble)) )
    //val countSums = counts.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2 ))
    //val keyMeans = countSums.mapValues(avgCount => avgCount._2 / avgCount._1)
    //keyMeans.foreach(println)

    """Autre Solution"""
    /*
    val pairRDD = suprdd.keyBy(elem => elem.split(";")(2))
    //pairRDD.foreach(println)
    val withValue1 = pairRDD.mapValues(e => (1.0, e))
    val countSums1 = withValue1.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2 ))
    val keyMeans1 = countSums1.mapValues(avgCount1 => avgCount1._2 / avgCount1._1)
    keyMeans1.foreach(println)
    */
    /*
    //session 2
    val df = sparkSession.read.option("inferSchema",true).option("header",true).csv("data/data.csv")
    //afficher les types des colonnes
    df.printSchema()

    //recupere que 2 colonnes du df
    val selectedDf = df.select("Region","country")

    import org.apache.spark.sql.functions._

    //filtrer sur le nom de la colonne
    //selectedDf.filter(col("Country") === "Chad").show

    //arrondie à val superieure
    val result = df.withColumn("Units_Sold_plus_1", col("Units Sold")+1)
    result.show

    //somme de colonnes
    val result2 = df.withColumn("Units_Sold_plus_Unit_Price", col("Units Sold")+col("Unit Price"))
    result2.show

    import sparkSession.implicits._
    val stringValues = df.select("Country").as[String].collect()
    //stringValues.foreach(println)
    stringValues.distinct.foreach(println)
    */

    //Question 1 : Lire le fichier en inférant les types
    val df = sparkSession.read.option("delimiter", ",").option("inferSchema", true).option("header", true).csv("data/data.csv")

    //Question 2 : Combien avons-nous de produits ayant un prix unitaire supérieur à 500 et plus de 3000 unités vendues ?
    val products = df.filter(col("Unit Price") >= 500).filter(col("Units Sold") >= 3000).count()
    println(products)
    //Question 3 : Faites la somme de tous les produits vendus valant plus de $500 en prix unitaire
    val sum = df.filter(col("Unit Price") >= 500).groupBy("Unit Price").sum("Units Sold")
    sum.show
    //Question 4 : Quel est le prix moyen de tous les produits vendus ? (En groupant par item_type)
    val mean = df.groupBy("Item Type").mean("Unit Price")
    mean.show


    //Question 5 : Créer une nouvelle colonne, "total_revenue" contenant le revenu total des ventes par produit vendu.
    val revenue = df.withColumn("total_revenue", col("Units Sold") * col("Unit Price"))
    //revenue.show

    //Question 6 : Créer une nouvelle colonne, "total_cost", contenant le coût total des ventes par produit vendu.
    val cost = revenue.withColumn("total_cost", col("Units Sold") * col("Unit Cost"))
    //cost.show

    //Note: on peut directement remplacer la colonne Total Profit déjà présente en la mettant dans withColumn
    //Question 7 : Créer une nouvelle colonne, "total_profit", contenant le bénéfice réalisé par produit.
    val profit = cost.withColumn("Total Profit", col("total_revenue") - col("total_cost"))
    profit.show

    //unit price discount
    val discount = profit.withColumn("unit_price_discount", when(col("Units Sold") > 3000, col("Unit Price") * 0.7).otherwise(col("Unit Price") * 0.9))
    //Question8 Créer une nouvelle colonne, "unit_price_discount", qui aura comme valeur "unit_price" si le nombre d'unités vendues est plus de 3000. Le montant du discount doit être de 20%.
    val q8 = discount.withColumn("total_revenue", col("Units Sold") * col("unit_price_discount"))
    //Question 9 Faire répercuter ça sur les 3 colonnes créées précédemment : total_revenue, total_cost et total_profit.
    val q9 = q8.withColumn("total_profit", when(col("Units Sold") > 3000, col("Units Sold") * (col("Unit Price") - col("Unit Cost"))))

    //10 Écrire le résultat sous format parquet, partitionné par formatted_ship_date


    //val column_without_spaces = df.columns.map(elem => elem.replaceAll(" ","_"))
    //column_without_spaces.foreach(println)

    //pour éviter erreur pour format parquet on enlève les espaces des noms de colonnes
    val columnsList = df.columns
    val columnsWithtoutSpaces: Array[String] = columnsList.map(elem => elem.replaceAll(" ", "_"))
    //Array("a", "b", "c") ==> "a", "b", "c"

    val dfWithRightColumnNames = df.toDF(columnsWithtoutSpaces: _*)
    //dfWithRightColumnNames.show()
    dfWithRightColumnNames.write.partitionBy("Sales_Channel").mode(SaveMode.Overwrite).parquet("result")
    val parquetDF = sparkSession.read.parquet("result")
    //parquetDF.printSchema()
    //parquetDF.show


    //Exo Supplementaire sur UDF
    // UDF -> User Defined Functions Exemple
    val transformField = udf((date: String) => {
      val formatDate = new SimpleDateFormat("yyyy-MM-dd")
      formatDate.format(new Date(date))
    })

    val transformField2 = udf((valeur: Int) => {
      val valeurdivpar2 = valeur / 2
      valeurdivpar2
    })

    //Usage: withcolumn pour juste appliquer une transformation pas pour add column
    //Usage:
    val transformedDateDf = df.withColumn("Order Date", transformField(col("Order Date")))
    transformedDateDf.show()

    //val transformedDateDf2 = df.withColumn("total_revenue", transformField2(col("total_revenu")))


  }
}