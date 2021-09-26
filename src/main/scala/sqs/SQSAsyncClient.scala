package sqs

import configuration.Configurations
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, AwsCredentialsProviderChain, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.model._
import software.amazon.awssdk.services.sqs.{SqsAsyncClient, SqsAsyncClientBuilder}

import java.net.URI
import java.util.UUID
import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class SQSAsyncClient(queueURL: String, regionOption: Option[String], endpointOption: Option[String])(implicit executionContext: ExecutionContext) {

    private val region: String = regionOption.getOrElse(Configurations.REGION)
    private val sqsClientBuilder: SqsAsyncClientBuilder = SqsAsyncClient
    .builder()
    .region(Region.of(region))
    .credentialsProvider(AwsCredentialsProviderChain.builder().credentialsProviders(StaticCredentialsProvider.create(AwsBasicCredentials.create(Configurations.AWS_ACCESS_KEY, Configurations.SECRET_ACCESS_KEY))).build())

    private val sqsAsyncClient: SqsAsyncClient = if (endpointOption.isDefined) {
        sqsClientBuilder
        .endpointOverride(URI.create(endpointOption.get))
        .build()
    }
    else {
        sqsClientBuilder.build()
    }

    def createStandardQueue(queueName: String): Future[CreateQueueResponse] = {
        val createQueueRequest = CreateQueueRequest.builder.queueName(queueName).build
        sqsAsyncClient.createQueue(createQueueRequest).toScala
    }

    //queueName should have a .fifo suffix
    def createFIFOQueue(queueName: String): Future[CreateQueueResponse] = {
        val attributeMap = Map((QueueAttributeName.FIFO_QUEUE, "true")).asJava
        val createQueueRequest = CreateQueueRequest.builder.queueName(queueName).attributes(attributeMap).build
        sqsAsyncClient.createQueue(createQueueRequest).toScala
    }

    def deleteQueue(): Future[DeleteQueueResponse] = {
        val deleteQueueRequest = DeleteQueueRequest.builder().queueUrl(queueURL).build()
        sqsAsyncClient.deleteQueue(deleteQueueRequest).toScala
    }

    def sendMessage(message: String): Future[SendMessageResponse] = {
        val sendMessageRequest = SendMessageRequest.builder().messageBody(message).queueUrl(queueURL).build()
        sqsAsyncClient.sendMessage(sendMessageRequest).toScala
    }

    def sendSMessagesInBatch(messages: List[String]): Future[SendMessageBatchResponse] = {
        val listSendMessageBatchRequestEntry = messages.map(SendMessageBatchRequestEntry.builder().messageBody(_).id(UUID.randomUUID().toString).build()).asJava
        val sendMessageBatchRequest = SendMessageBatchRequest.builder().queueUrl(queueURL).entries(listSendMessageBatchRequestEntry).build()
        sqsAsyncClient.sendMessageBatch(sendMessageBatchRequest).toScala
    }

    /*
        maxNumberOfMessages can not be more than 10.
     */
    def receiveMessages(maxNumberOfMessages: Int): Future[ReceiveMessageResponse] = {
        val receiveMessageRequest = ReceiveMessageRequest.builder().maxNumberOfMessages(maxNumberOfMessages).queueUrl(queueURL).waitTimeSeconds(10).build()
        sqsAsyncClient.receiveMessage(receiveMessageRequest).toScala
    }

    def deleteMessage(receiptHandle: String): Future[DeleteMessageResponse] = {
        val deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(queueURL).receiptHandle(receiptHandle).build()
        sqsAsyncClient.deleteMessage(deleteMessageRequest).toScala
    }

    def deleteMessageInBatch(messages: List[Message]): Future[DeleteMessageBatchResponse] = {
        val listDeleteMessageBatchRequestEntry = messages.map(message => DeleteMessageBatchRequestEntry.builder().receiptHandle(message.receiptHandle()).build()).asJava
        val deleteMessageBatchRequest = DeleteMessageBatchRequest.builder().queueUrl(queueURL).entries(listDeleteMessageBatchRequestEntry).build()
        sqsAsyncClient.deleteMessageBatch(deleteMessageBatchRequest).toScala
    }

    def getQueueURL(queueName: String): Future[GetQueueUrlResponse] = {
        val getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(queueName).build()
        sqsAsyncClient.getQueueUrl(getQueueUrlRequest).toScala
    }

    def listQueues(): Future[ListQueuesResponse] = {
        sqsAsyncClient.listQueues().toScala
    }

    def listQueuesStartingFromPrefix(prefix: String): Future[ListQueuesResponse] = {
        val listQueueStartingFromPrefixRequest = ListQueuesRequest.builder().queueNamePrefix(prefix).build()
        sqsAsyncClient.listQueues(listQueueStartingFromPrefixRequest).toScala
    }

    def changeMessageVisibility(message: Message): Future[ChangeMessageVisibilityResponse] = {
        val changeMessageVisibilityRequest = ChangeMessageVisibilityRequest.builder().queueUrl(queueURL).receiptHandle(message.receiptHandle()).visibilityTimeout(30).build()
        sqsAsyncClient.changeMessageVisibility(changeMessageVisibilityRequest).toScala
    }

    def changeMessageVisibilityOfBatch(messages: List[Message]): Future[ChangeMessageVisibilityBatchResponse] = {
        val changeMessageVisibilityBatchRequestEntry = messages.map(message => ChangeMessageVisibilityBatchRequestEntry.builder().receiptHandle(message.receiptHandle()).visibilityTimeout(30).build()).asJava
        val changeMessageVisibilityRequest = ChangeMessageVisibilityBatchRequest.builder().queueUrl(queueURL).entries(changeMessageVisibilityBatchRequestEntry).build()
        sqsAsyncClient.changeMessageVisibilityBatch(changeMessageVisibilityRequest).toScala
    }

    def purgeQueue(): Future[PurgeQueueResponse] = {
        val purgeQueueRequest = PurgeQueueRequest.builder().queueUrl(queueURL).build()
        sqsAsyncClient.purgeQueue(purgeQueueRequest).toScala
    }

    def setQueueAttributes(attributes: Map[QueueAttributeName, String]): Future[SetQueueAttributesResponse] = {
        val setQueueAttributesRequest = SetQueueAttributesRequest.builder().queueUrl(queueURL).attributes(attributes.asJava).build()
        sqsAsyncClient.setQueueAttributes(setQueueAttributesRequest).toScala
    }

    def tagQueue(tags: Map[String, String]): Future[TagQueueResponse] = {
        val tagQueueRequest = TagQueueRequest.builder().queueUrl(queueURL).tags(tags.asJava).build()
        sqsAsyncClient.tagQueue(tagQueueRequest).toScala
    }

    def untagQueue(listOfTagsToRemove: List[String]): Future[UntagQueueResponse] = {
        val untagQueueRequest = UntagQueueRequest.builder().queueUrl(queueURL).tagKeys(listOfTagsToRemove.asJava).build()
        sqsAsyncClient.untagQueue(untagQueueRequest).toScala
    }

}
