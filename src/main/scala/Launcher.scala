import org.apache.log4j.{LogManager, Logger}
import java.util.Properties

import com.twitter.hbc.core.endpoint.Location

import scala.io.Source

object Launcher {

  private var logger = LogManager.getLogger("Log_Console")

  val url = getClass.getResource("streaming.properties")
  val source = Source.fromURL(url) // Source ici = Source de Scala.io
  val properties : Properties = new Properties
    properties.load(source.bufferedReader())

  val consumer_key = properties.getProperty("consumerKey")
  val consumer_secret = properties.getProperty("consumerSecret")
  val access_token = properties.getProperty("token")
  val token_Secret = properties.getProperty("tokenSecret")
  val server_kafka = properties.getProperty("bootStrapServers")
  val topic_lecture = properties.getProperty("topic")

  val groupId = properties.getProperty("consumerGroupId")
  val readOrder = properties.getProperty("consumerReadOrder")
  val sparkBatchDuration = properties.getProperty("batchDuration")
  val mySQLHost = properties.getProperty("mySQLHost")
  val mySQLUser = properties.getProperty("mySQLUser")
  val mySQLPwd = properties.getProperty("mySQLPwd")
  val mySQLDatabase = properties.getProperty("mySQLDatabase")

  // Coordonnées géographiques des pays

  val lilleTweets = new Location(new Location.Coordinate(2.967968,50.600826), new Location.Coordinate(2.967968,50.600826))

  twitterClient.getClient_Twitter(access_token, consumer_secret, consumer_key, token_Secret, lilleTweets, topic_lecture, server_kafka)


}
