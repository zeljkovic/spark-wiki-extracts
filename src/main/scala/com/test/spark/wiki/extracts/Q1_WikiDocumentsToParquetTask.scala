package com.test.spark.wiki.extracts

import java.io.FileReader

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.joda.time.DateTime
import org.jsoup.Jsoup
import org.jsoup.select.Elements
import org.slf4j.{Logger, LoggerFactory}

case class Q1_WikiDocumentsToParquetTask(bucket: String) extends Runnable {
  private val sparkConfig: SparkConf = new SparkConf().setAppName("spark-wiki-extracts-zeljkovic")
  private val session: SparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def run(): Unit = {
    val toDate = DateTime.now().withYear(2017)
    val fromDate = toDate.minusYears(40)
    import session.implicits._
    getLeagues
      // TODO Q1 Transformer cette seq en dataset
      .toDS()
      .flatMap {
        input =>
          (fromDate.getYear until toDate.getYear).map {
            year =>
              year + 1 -> (input.name, input.url.format(year, year + 1))
          }
      }
      .flatMap {
        case (season, (league, url)) =>
          try {
            // TODO Q2 Implémenter le parsing Jsoup. Il faut retourner une Seq[LeagueStanding]
            // ATTENTION:
            //  - Quelques pages ne respectent pas le format commun et pourraient planter - pas grave
            //  - Il faut veillez à recuperer uniquement le classement général
            //  - Il faut normaliser les colonnes "positions", "teams" et "points" en cas de problèmes de formatage
            val doc = Jsoup.connect(url).get
            val table = doc.select("caption:contains(Classement)").first().parent()
            val rows: Elements = table.select("tr")
            for (i <- 1 to rows.size() -1)
              yield {
                val row = rows.get(i)
                val tds = row.select("td")
                val position  = tds.get(0).text.toInt
                val team = tds.get(1).text
                val points = tds.get(2).text.toInt
                val played = tds.get(3).text.toInt
                val won = tds.get(4).text.toInt
                val drawn = tds.get(5).text.toInt
                val lost = tds.get(6).text.toInt
                val goalsFor = tds.get(7).text.toInt
                val goalsAgainst = tds.get(8).text.toInt
                val goalsDifference = tds.get(9).text.toInt
                LeagueStanding(league,season,position,team,points,played,won,drawn,lost,goalsFor,goalsAgainst,goalsDifference)
              }
          } catch {
            case _: Throwable =>
              // TODO Q3 En supposant que ce job tourne sur un cluster EMR, où seront affichés les logs d'erreurs ?
              //sais pas
              logger.warn(s"Can't parse season $season from $url")
              Seq.empty
          }
      }
      // TODO Q4 Comment partitionner les données en 2 avant l'écriture sur le bucket
      .coalesce(2)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(bucket)

    // TODO Q5 Quel est l'avantage du format parquet par rapport aux autres formats ?
    //schema inclus
    //format colonne
    //compressé

    // TODO Q6 Quel est l'avantage de passer d'une séquence scala à un dataset spark ?
    //paralléliser le traitement
  }

  private def getLeagues = {
    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
    val inputStream = new FileReader(getClass.getResource("/leagues.yaml").toURI.getPath)
    // TODO Q7 Recuperer l'input stream du fichier leagues.yaml
    mapper.readValue(inputStream, classOf[Array[LeagueInput]]).toSeq
  }
}


// TODO Q8 Ajouter les annotations manquantes pour pouvoir mapper le fichier yaml à cette classe
case class LeagueInput(@JsonProperty("name") name: String,
                       @JsonProperty("url") url: String)

case class LeagueStanding(league: String,
                          season: Int,
                          position: Int,
                          team: String,
                          points: Int,
                          played: Int,
                          won: Int,
                          drawn: Int,
                          lost: Int,
                          goalsFor: Int,
                          goalsAgainst: Int,
                          goalsDifference: Int)
