
import com.twitter.hbc.httpclient.auth._
import com.twitter.hbc.core.{Client, Constants}
import com.twitter.hbc.core.endpoint.{StatusesFilterEndpoint, Location}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import producerKafka._

import scala.collection.JavaConverters._
import org.apache.log4j.{LogManager, Logger}

object twitterClient {

  /*
  ************** 3 étapes : authenfication, récupération des messages spécifiques et construction d''un client hbc
   */
 private val trace_hbc = LogManager.getLogger("console")


  /*
  *************** 4 élements essentiels pour authentification Twitter sont les suivants en variables
   */

  /*
  Création d'une méthode pour prendre les élements d'authentification et de location en compte
   */

  def getClient_Twitter(token: String, consumerSecret: String, consumerKey: String, tokenSecret: String, town: Location, topicKafka : String, server_kafka : String ): Unit = {
    val token = ""
    val consumerSecret = ""
    val consumerKey = ""
    val tokenSecret = ""


    val queue : BlockingQueue[String] = new LinkedBlockingQueue[String](1000000)
    val auth : Authentication = new OAuth1(consumerKey, consumerSecret, token, tokenSecret)

    val endp : StatusesFilterEndpoint = new StatusesFilterEndpoint()
    endp.locations(List(town).asJava)
    /*
    *************** Récupération de la data *********************
     */
    val client_HBC = new ClientBuilder()
      .hosts(Constants.STREAM_HOST)
      .authentication(auth)
      .gzipEnabled(true)
      .endpoint(endp)
      .processor(new StringDelimitedProcessor(queue)
      )

    val client_HBC_complete = client_HBC.build()

    client_HBC_complete.connect()

    try {
      while (!client_HBC_complete.isDone) {
        val tweet = queue.poll(300, TimeUnit.SECONDS)
        getProducerKafka(tweet, topicKafka, server_kafka, town)
      }

    }catch {
     case ex : Exception => trace_hbc.error(s"erreur dans le client HBC. Détails : ${ex.printStackTrace()}")
    } finally {
      client_HBC_complete.stop()
    }

  }

}


