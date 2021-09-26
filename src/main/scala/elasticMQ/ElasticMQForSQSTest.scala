package elasticMQ

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import configuration.Configurations
import org.elasticmq.server.ElasticMQServer
import org.elasticmq.server.config.ElasticMQServerConfig
import sqs.SQSAsyncClient

import scala.util.{Failure, Success}

object ElasticMQForSQSTest extends App {
    implicit val actorSystem = ActorSystem.create()
    implicit val executionContext = actorSystem.dispatcher
    implicit val m_logger = actorSystem.log

    val queueName = "queue1"
    val queueURL = s"http://localhost:9324/000000000000/$queueName"
    val config = ConfigFactory.load("elasticmq.conf")
    val server = new ElasticMQServer(new ElasticMQServerConfig(config))
    server.start()

    val endpoint = "http://localhost:9324"
    val region = "elasticmq"

    val elasticMQSQSClient = new SQSAsyncClient(queueURL, Some(Configurations.ELASTIC_MQ_REGION), Some(Configurations.ELASTIC_MQ_ENDPOINT))

    elasticMQSQSClient.createStandardQueue("standardQueueForTest").onComplete {
        case Success(_) => m_logger.info("queue created")
        case Failure(exception) => m_logger.error(exception, "exception in queue creation")
    }

    elasticMQSQSClient.createFIFOQueue("fifoQueue.fifo").onComplete {
        case Success(_) => m_logger.info("queue created")
        case Failure(exception) => m_logger.error(exception, "exception in queue creation")
    }

    elasticMQSQSClient.listQueues().onComplete {
        case Success(value) => m_logger.info(s"queues : ${value.queueUrls()}")
        case Failure(exception) => m_logger.error(exception, "exception in queue listing")
    }

    elasticMQSQSClient.sendMessage("Hi").onComplete {
        case Success(_) => m_logger.info("message sent successfully")
        case Failure(exception) => m_logger.error(exception, "exception in sending message")
    }

    elasticMQSQSClient.sendSMessagesInBatch(List("Hey", "message", "in", "batch")).onComplete {
        case Success(value) => m_logger.info(s"messages sent succesfully : ${value.successful()}")
        case Failure(exception) => m_logger.error(exception, "exception in sending messages")
    }

    elasticMQSQSClient.receiveMessages(5).onComplete {
        case Success(value) => m_logger.info(s"messages received : ${value.messages()}")
        case Failure(exception) => m_logger.error(exception, "exception in receiving messages")
    }

    elasticMQSQSClient.purgeQueue().onComplete {
        case Success(_) => m_logger.info(s"queue purged")
        case Failure(exception) => m_logger.error(exception, "exception in purging queue")
    }
}
