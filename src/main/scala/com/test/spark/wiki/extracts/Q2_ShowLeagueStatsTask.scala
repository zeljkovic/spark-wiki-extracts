package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

case class Q2_ShowLeagueStatsTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().getOrCreate()

  import session.implicits._

  override def run(): Unit = {
    val standings = session.read.parquet(bucket).as[LeagueStanding].cache()

    // TODO Répondre aux questions suivantes en utilisant le dataset $standings
    standings
      // ...code...
      .show(false)

    // TODO Q1
    println("Utiliser createTempView sur $standings et, en sql, afficher la moyenne de buts par saison et " +
      "par championnat")
    standings.createOrReplaceTempView("standingsSQL")
    session.sql("select season,league,round(mean(goalsFor),2) mean_goals from standingsSQL group by season,league order by season,league")
      .show(false)

    // TODO Q2
    println("En Dataset, quelle est l'équipe la plus titrée de France ?")
    val w = Window.orderBy(col("count").desc)
    standings
      .filter(col("league") === "Ligue 1")
      .filter(col("position") === "1")
      .groupBy("team")
      .count
      .orderBy(desc("count"))
      .select(col("team"),col("count"), rank.over(w).alias("rank"))
      .filter(col("rank") === "1")
      .show(false)

    // TODO Q3
    println("En Dataset, quelle est la moyenne de points des vainqueurs sur les 5 différents championnats ?")
    standings
      .filter(col("position") === "1")
      .groupBy(col("league"))
      .agg(mean(col("points")).alias("mean points"))
      .show(false)
    // TODO Q5 Ecrire une udf spark "decade" qui retourne la décennie d'une saison sous la forme 199X ?

    import org.apache.spark.sql.functions.udf
    val decade: Int => String = {year => year.toString.substring(0,3) + "X"}
    val decadeUDF = udf(decade)
    standings.select(decadeUDF(col("season")))
      .show(false)

    // TODO Q4
    println("En Dataset, quelle est la moyenne de points d'écart entre le 1er et le 10ème de chaque championnats " +
      "par décennie")

    standings
      .filter(col("position") === "1" || col("position") === "10")
      .groupBy(col("league"))
      .pivot("position")
      .mean("points")
      .withColumn("mean delta",col("1")-col("10"))
      .show(false)
  }
}
