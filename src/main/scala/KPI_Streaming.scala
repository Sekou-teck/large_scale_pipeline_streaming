
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.security.auth.SecurityProtocol._
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import Launcher._
/*
Ce module comporte :
    ==> construction de Spark Streaming = pour consommer les données des clusters ;
    ==> construction de Spark SQL = pour calculer les indicateurs et
    ==> persister les résultats dans une BD distante MySQL.
 */

object KPI_Streaming {
  val  kpiSchema = StructType ( Array(        // Les schémas pour les données twitter car elles sont récupérées en étant Objet.
    StructField("event_date", DateType, true),
    StructField("id", StringType, false),
    StructField("text", StringType, true),
    StructField("lang", StringType, true),
    StructField("userid", StringType, false),
    StructField("name", StringType, false),
    StructField("screenName", StringType, true),
    StructField("location", StringType, true),
    StructField("followersCount", IntegerType, false),
    StructField("retweetCount", IntegerType, false),
    StructField("favoriteCount", IntegerType, false),
    StructField("Zipcode", StringType, true),
    StructField("ZipCodeType", StringType, true),
    StructField("City", StringType, true),
    StructField("State", StringType, true)
  )

  )

  // Ce sont les paramètres de Spark Consumer, client que l'on a besoin pour le connecter à Kafka.
  private val bootStrapServers : String = Launcher.server_kafka
  private val consumerGroupId : String = Launcher.groupId
  private val consumerReadOrder : String = Launcher.readOrder

  // private val kerberosName : String = ""
  private val batchDuration  = 300
  private val topics : Array[String] = Array(Launcher.topic_lecture)
  private val mySQLHost = Launcher.mySQLHost
  private val mySQLUser = Launcher.mySQLUser
  private val mySQLPwd = Launcher.mySQLPwd
  private val mySQLDatabase = Launcher.mySQLDatabase

  private var logger : Logger = LogManager.getLogger("Log_Console")
  var ss : SparkSession = null
  var spConf : SparkConf = null

  // Application qui va se connecter

  def main(args: Array[String]): Unit = {

    val kafkaParam = Map(
      "boostrap.servers" -> bootStrapServers,
      "group.id" -> consumerGroupId,
      "enable.auto.commit" -> (false : java.lang.Boolean),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "security.protocol" -> SecurityProtocol.SASL_PLAINTEXT
      //"security.kerberos.service.name" -> "" // Au cas où on aurait utilisé Kerberos
    )
    // Une instance de Kafka consumer qui se connecte à Spark

    val ssc = getSparkStreamingContext(batchDuration, env= true)

    try {

      // Kafka consumer

      val kk_consumer = KafkaUtils.createDirectStream[String, String](
        ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParam)
      )

      // Lecture des évenements reçus de Kafka

      kk_consumer.foreachRDD{
        rdd_kafka => {
          val record_kafka = rdd_kafka.map(c => c.value()) // rappel : offset = position de message
          val offsets = rdd_kafka.asInstanceOf[HasOffsetRanges].offsetRanges // lecture terminée à ce stade

          // Format des données en Spark étant les RDD en Json, pour les lire en MySQL, je crée une session SQL

          val s_session = SparkSession.builder()
            .config(rdd_kafka.sparkContext.getConf)
            .enableHiveSupport()
            .getOrCreate()

          import s_session.implicits._
          val df_messages = record_kafka.toDF("kafka_object") // toDF est valable avec import implicits

          // il faut formater le nom des colonnes de Json afin d'avoir la tabulation dans le DF

          if(df_messages.count() > 0) {

            // 1- parser les données
            val df_parsed = messagePersing(df_messages)
            // 2- calculer des indicateurs
            val df_indicateurs = computeIndicators(df_parsed, s_session).cache() // pour calibrer la mémoire.

            // 3- écrire des indicateurs dans MySQL
            df_indicateurs.write
              .format("jdbc")
              .mode(SaveMode.Append) // Append = ajouts, Overwait = écraser les anciens.
              .option("url", s"jdbc:mysql://${mySQLHost}?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC")
              .option("dbtable", mySQLDatabase)
              .option("user", mySQLUser)
              .option("password", mySQLPwd)
              .save()
          }

          // 4 - Sinon, je dis à Kafka que mon traitement est terminé.

          kk_consumer.asInstanceOf[CanCommitOffsets].commitAsync(offsets) // Cette ligne quarantie la schémantique du traitement :
          // exactement une fois et au moins une fois
          // Il faut ces schémantiques, sinon, en cas de panne, Au plus une fois sera automatique.
          // Cela renvoi même les anciens déjà lus. Un pb streaming

        }     // expression lambda en Java 8 ou fonction anonyme en Scala

      }

    } catch {

      case ex : Exception => logger.error(s"l'application Spark a rencontré une erreur fatale : ${ex.printStackTrace()}")
    } finally {

      ssc.start()
      ssc.awaitTermination()

    }

  }
  // Initialisation de certains paramètres :

  def getSparkStreamingContext(duree_batch : Int, env : Boolean) : StreamingContext = {
    logger.info("initialisation du contexte Spark Streaming")
    if(env) { // si Spark est déployé en local
      spConf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("Mon app Streaming Dev")
    } else {
      spConf = new SparkConf().setAppName("MOn app Streaming Prod")
    }

  val ssc : StreamingContext = new StreamingContext(spConf, Seconds(duree_batch))

  return ssc

  }

  def messagePersing(df_kafkaEvents : DataFrame) : DataFrame = {
    logger.info("parsing des objets Json encours de réception de Kafka")

    val df_parsed = df_kafkaEvents.withColumn("kafka_objet", from_json(col("kafka_objet"), kpiSchema))

    df_parsed

  }

  def computeIndicators(events_parsed : DataFrame, ss : SparkSession) : DataFrame = {
logger.info("calcul des KPI encours...")
events_parsed.createTempView("messages")

val df_kpi = ss.sql("""
        SELECT t.City,
               count(t.id) OVER (PARTITION BY t.City ORDER BY City) as tweetCount,
               sum(t.bin_retweet) OVER (PARTITION BY t.City ORDER BY City) as retweetCount
        FROM  (
             SELECT date_event, id, City, CASE WHEN retweetCount > 0 THEN 1 ELSE 0 END AS bin_retweet  FROM events_tweets
              ) t
    """
    ).withColumn("date_event", current_timestamp())
      .select(
        col("city").alias("date de l'évenement"),
        col("city").alias("events city"),
        col("tweetCount").alias("Nbr de tweets par cité"),
        col("retweetCount").alias("Nbr de RT par cité")
      )
    return df_kpi
  }

}

// SparkLauncher = Déploiement en PROD Kafka Consumer : il se fait en variablissant les paramètres et au format .sh