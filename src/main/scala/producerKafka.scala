
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.kafka.common.serialization._
import org.apache.log4j.{LogManager, Logger}
import java.util._

import com.twitter.hbc.core.endpoint.Location
import org.apache.kafka.common.security.auth.SecurityProtocol

/*
  ************* 'Extends App' me permet de ne plus passer par une classe main. */

object producerKafka /* extends App */ {

  private val trace_kafka = LogManager.getLogger("console")

  def getProducerKafka (message : String, topic : String, serversIP : String, ville : Location): Unit ={
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serversIP)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put("security.protocol", "SASL_PLAINTEXT")

    /*
    ******************* Gestionnaire d''erreurs *******************
     */
    try
      {
        val twitterProducer = new KafkaProducer[String, String](props)
        val twitterRecord = new ProducerRecord[String, String](topic, ville.toString, message)
        twitterProducer.send(twitterRecord)
      }
    catch {
      /*
      ********************* Envoi du message d''erreur *****************
      */
      case ex : Exception => trace_kafka.error(s"erreur dans l''envoi du message. Détails de l'erreur : ${ex.printStackTrace()}")
    }

  }


}
