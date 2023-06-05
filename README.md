Spring Integration Extension for Amazon Web Services (AWS)
==========================================================

# Introduction

## Amazon Web Services (AWS)

Launched in 2006, [Amazon Web Services][] (AWS) provides key infrastructure services for business through its cloud computing platform. 
Using cloud computing businesses can adopt a new business model whereby they do not have to plan and invest in procuring their own IT infrastructure. 
They can use the infrastructure and services provided by the cloud service provider and pay as they use the services. Visit [AWS Products](https://aws.amazon.com/products/) for more details about various products offered by Amazon as a part their cloud computing services.

*Spring Integration Extension for Amazon Web Services* provides Spring Integration adapters for the various services provided by the [AWS SDK for Java][].
Note the Spring Integration AWS Extension is based on the [Spring Cloud AWS][] project.

## Spring Integration's extensions to AWS

The current project version is `3.0.x` and it requires minimum Java `17` and Spring Integration `6.1.x` & Spring Cloud AWS `3.0.x`.
This version is also fully based on AWS Java SDK v2.
Therefore, it has a lot of breaking changes since the previous version, for example an XML configuration support was fully removed. 

This guide intends to explain briefly the various adapters available for [Amazon Web Services][] such as:

* **Amazon Simple Storage Service (S3)**
* **Amazon Simple Queue Service (SQS)**
* **Amazon Simple Notification Service (SNS)**
* **Amazon DynamoDB**
* **Amazon Kinesis**

## Contributing

[Pull requests][] are welcome. 
Please see the [contributor guidelines][] for details. 
Additionally, if you are contributing, we recommend following the process for Spring Integration as outlined in the [administrator guidelines][].

# Dependency Management

These dependencies are optional in the project:

* `io.awspring.cloud:spring-cloud-aws-sns` - for SNS channel adapters
* `io.awspring.cloud:spring-cloud-aws-sqs` - for SQS channel adapters
* `io.awspring.cloud:spring-cloud-aws-s3` - for S3 channel adapters
* `org.springframework.integration:spring-integration-file` - for S3 channel adapters
* `org.springframework.integration:spring-integration-http` - for SNS inbound channel adapter
* `software.amazon.awssdk:kinesis` - for Kinesis channel adapters
* `software.amazon.kinesis:amazon-kinesis-client` - for KCL-based inbound channel adapter 
* `com.amazonaws:amazon-kinesis-producer` - for KPL-based `MessageHandler` 
* `software.amazon.awssdk:dynamodb` - for `DynamoDbMetadataStore` and `DynamoDbLockRegistry`
* `software.amazon.awssdk:s3-transfer-manager` - for `S3MessageHandler`

Consider to include an appropriate dependency into your project when you use particular component from this project. 

# Adapters

## Amazon Simple Storage Service (Amazon S3)

### Introduction

The S3 Channel Adapters are based on the `AmazonS3` template and `TransferManager`.
See their specification and JavaDocs for more information.

### Inbound Channel Adapter

The S3 Inbound Channel Adapter is represented by the `S3InboundFileSynchronizingMessageSource` and allows pulling S3 objects as files from the S3 bucket to the local directory for synchronization.
This adapter is fully similar to the Inbound Channel Adapters in the FTP and SFTP Spring Integration modules.
See more information in the [FTP/FTPS Adapters Chapter][] for common options or `SessionFactory`, `RemoteFileTemplate` and `FileListFilter` abstractions.

The Java Configuration is:

````java
@SpringBootApplication
public static class MyConfiguration {

    @Autowired
    private S3Client amazonS3;

    @Bean
    public S3InboundFileSynchronizer s3InboundFileSynchronizer() {
    	S3InboundFileSynchronizer synchronizer = new S3InboundFileSynchronizer(amazonS3());
    	synchronizer.setDeleteRemoteFiles(true);
    	synchronizer.setPreserveTimestamp(true);
    	synchronizer.setRemoteDirectory(S3_BUCKET);
    	synchronizer.setFilter(new S3RegexPatternFileListFilter(".*\\.test$"));
    	Expression expression = PARSER.parseExpression("#this.toUpperCase() + '.a'");
    	synchronizer.setLocalFilenameGeneratorExpression(expression);
    	return synchronizer;
    }

    @Bean
    @InboundChannelAdapter(value = "s3FilesChannel", poller = @Poller(fixedDelay = "100"))
    public S3InboundFileSynchronizingMessageSource s3InboundFileSynchronizingMessageSource() {
    	S3InboundFileSynchronizingMessageSource messageSource =
    			new S3InboundFileSynchronizingMessageSource(s3InboundFileSynchronizer());
    	messageSource.setAutoCreateLocalDirectory(true);
    	messageSource.setLocalDirectory(LOCAL_FOLDER);
    	messageSource.setLocalFilter(new AcceptOnceFileListFilter<File>());
    	return messageSource;
    }

    @Bean
    public PollableChannel s3FilesChannel() {
    	return new QueueChannel();
    }
}
````

With this config you receive messages with `java.io.File` `payload` from the `s3FilesChannel` after periodic synchronization of content from the Amazon S3 bucket into the local directory.

### Streaming Inbound Channel Adapter

This adapter produces message with payloads of type `InputStream`, allowing S3 objects to be fetched without writing to the local file system. 
Since the session remains open, the consuming application is responsible for closing the session when the file has been consumed. 
The session is provided in the closeableResource header (`IntegrationMessageHeaderAccessor.CLOSEABLE_RESOURCE`). 
Standard framework components, such as the `FileSplitter` and `StreamTransformer` will automatically close the session.
 
The following Spring Boot application provides an example of configuring the S3 inbound streaming adapter using Java configuration:

````java
@SpringBootApplication
public class S3JavaApplication {

    public static void main(String[] args) {
        new SpringApplicationBuilder(S3JavaApplication.class)
            .web(false)
            .run(args);
    }
    
    @Autowired
    private S3Client amazonS3;

    @Bean
    @InboundChannelAdapter(value = "s3Channel", poller = @Poller(fixedDelay = "100"))
    public MessageSource<InputStream> s3InboundStreamingMessageSource() {    
        S3StreamingMessageSource messageSource = new S3StreamingMessageSource(template());
        messageSource.setRemoteDirectory(S3_BUCKET);
        messageSource.setFilter(new S3PersistentAcceptOnceFileListFilter(new SimpleMetadataStore(),
                                   "streaming"));    	
    	return messageSource;
    }

    @Bean
    @Transformer(inputChannel = "s3Channel", outputChannel = "data")
    public org.springframework.integration.transformer.Transformer transformer() {
        return new StreamTransformer();
    }
    
    @Bean
    public S3RemoteFileTemplate template() {
        return new S3RemoteFileTemplate(new S3SessionFactory(amazonS3));
    }

    @Bean
    public PollableChannel s3Channel() {
    	return new QueueChannel();
    }
}
````

> NOTE: Unlike the non-streaming inbound channel adapter, this adapter does not prevent duplicates by default. 
> If you do not delete the remote file and wish to prevent the file being processed again, you can configure an `S3PersistentFileListFilter` in the `filter` attribute. 
> If you don’t actually want to persist the state, an in-memory `SimpleMetadataStore` can be used with the filter. 
> If you wish to use a filename pattern (or regex) as well, use a `CompositeFileListFilter`.

### Outbound Channel Adapter

The S3 Outbound Channel Adapter is represented by the `S3MessageHandler` and allows performing `upload`, `download` and `copy` (see `S3MessageHandler.Command` enum) operations in the provided S3 bucket.

The Java Configuration is:

````java
@SpringBootApplication
public static class MyConfiguration {

    @Autowired
    private S3AsyncClient amazonS3;

    @Bean
    @ServiceActivator(inputChannel = "s3UploadChannel")
    public MessageHandler s3MessageHandler() {
    	return new S3MessageHandler(amazonS3(), "my-bucket");
    }

}
````

With this config you can send a message with the `java.io.File` as `payload` and the `transferManager.upload()` operation will be performed, where the file name is used as a S3 Object key.

See more information in the `S3MessageHandler` JavaDocs.

### Outbound Gateway

The S3 Outbound Gateway is represented by the same `S3MessageHandler` with the `produceReply = true` constructor argument for Java Configuration.

The "request-reply" nature of this gateway is async and the `Transfer` result from the `TransferManager` operation is sent to the `outputChannel`, assuming the transfer progress observation in the downstream flow.

The `TransferListener` can be supplied via `AwsHeaders.TRANSFER_LISTENER` header of the request message to track the transfer progress.

See more information in the `S3MessageHandler` JavaDocs.

## Simple Email Service (SES)

There is no adapter for SES, since [Spring Cloud AWS][] provides implementations for `org.springframework.mail.MailSender` - `SimpleEmailServiceMailSender` and `SimpleEmailServiceJavaMailSender`, which can be injected to the `MailSendingMessageHandler`.

## Amazon Simple Queue Service (SQS)

The `SQS` adapters are fully based on the [Spring Cloud AWS][] foundation, so for more information about the background components and core configuration, please, refer to the documentation of that project.

### Outbound Channel Adapter

The SQS Outbound Channel Adapter is presented by the `SqsMessageHandler` implementation and allows sending messages to the SQS `queue` with provided `SqsAsyncClient` client. 
An SQS queue can be configured explicitly on the adapter (using `org.springframework.integration.expression.ValueExpression`) or as a SpEL `Expression`, which is evaluated against request message as a root object of evaluation context. 
In addition, the `queue` can be extracted from the message headers under `AwsHeaders.QUEUE`.

The Java Configuration is pretty simple:

````java
@SpringBootApplication
public static class MyConfiguration {

    @Bean
    @ServiceActivator(inputChannel = "sqsSendChannel")
    public MessageHandler sqsMessageHandler(SqsAsyncClient amazonSqs) {
    	return new SqsMessageHandler(amazonSqs);
    }

}
````

Starting with _version 2.0_, the `SqsMessageHandler` can be configured with the `HeaderMapper` to map message headers to the SQS message attributes.
See `SqsHeaderMapper` implementation for more information and also consult with [Amazon SQS Message Attributes][] about value types and restrictions.   

### Inbound Channel Adapter

The SQS Inbound Channel Adapter is a `message-driven` implementation for the `MessageProducer` and is represented with `SqsMessageDrivenChannelAdapter`. 
This channel adapter is based on the `io.awspring.cloud.sqs.listener.SqsMessageListenerContainer` to receive messages from the provided `queues` in async manner and send an enhanced Spring Integration Message to the provided `MessageChannel`.

The Java Configuration is pretty simple:

````java
@SpringBootApplication
public static class MyConfiguration {

	@Autowired
	private SqsAsyncClient amazonSqs;

	@Bean
	public PollableChannel inputChannel() {
		return new QueueChannel();
	}

	@Bean
	public MessageProducer sqsMessageDrivenChannelAdapter() {
		SqsMessageDrivenChannelAdapter adapter = new SqsMessageDrivenChannelAdapter(this.amazonSqs, "myQueue");
		adapter.setOutputChannel(inputChannel());
		return adapter;
	}
}
````

The target listener container can be configured via `SqsMessageDrivenChannelAdapter.setSqsContainerOptions(SqsContainerOptions)` option. 

## Amazon Simple Notification Service (SNS)

Amazon SNS is a publish-subscribe messaging system that allows clients to publish notification to a particular topic.
Other interested clients may subscribe using different protocols like HTTP/HTTPS, e-mail, or an Amazon SQS queue to receive the messages. 
Plus mobile devices can be registered as subscribers from the AWS Management Console.

Unfortunately [Spring Cloud AWS][] doesn't provide flexible components which can be used from the channel adapter implementations, but Amazon SNS API is pretty simple, on the other hand. 
Hence, Spring Integration AWS SNS Support is straightforward and just allows to provide channel adapter foundation for Spring Integration applications.

Since e-mail, SMS and mobile device subscription/unsubscription confirmation is out of the Spring Integration application scope and can be done only from the AWS Management Console, we provide only HTTP/HTTPS SNS endpoint in face of `SnsInboundChannelAdapter`. 
The SQS-to-SNS subscription can also be done through account configuration: https://docs.aws.amazon.com/sns/latest/dg/subscribe-sqs-queue-to-sns-topic.html.

### Inbound Channel Adapter

The `SnsInboundChannelAdapter` is an extension of `HttpRequestHandlingMessagingGateway` and must be as a part of Spring MVC application. 
Its URL must be used from the AWS Management Console to add this endpoint as a subscriber to the SNS Topic. 
However. before receiving any notification itself this HTTP endpoint must confirm the subscription.

See `SnsInboundChannelAdapter` JavaDocs for more information.

An important option of this adapter to consider is `handleNotificationStatus`. 
This `boolean` flag indicates if the adapter should send `SubscriptionConfirmation/UnsubscribeConfirmation` message to the `output-channel` or not. 
If that the `AwsHeaders.NOTIFICATION_STATUS` message header is present in the message with the `NotificationStatus` object, which can be used in the downstream flow to confirm subscription or not. 
Or "re-confirm" it in case of `UnsubscribeConfirmation` message.

In addition, the `AwsHeaders#SNS_MESSAGE_TYPE` message header is represented to simplify a routing in the downstream flow.

The Java Configuration is pretty simple:

````java
@SpringBootApplication
public static class MyConfiguration {

	@Autowired
	private SnsClient amazonSns;

	@Bean
    public PollableChannel inputChannel() {
    	return new QueueChannel();
    }

    @Bean
    public HttpRequestHandler sqsMessageDrivenChannelAdapter() {
    	SnsInboundChannelAdapter adapter = new SnsInboundChannelAdapter(amazonSns(), "/mySampleTopic");
    	adapter.setRequestChannel(inputChannel());
    	adapter.setHandleNotificationStatus(true);
    	return adapter;
    }
}
````

Note: by default the message `payload` is a `Map` converted from the received Topic JSON message. 
For the convenience a `payload-expression` is provided with the `Message` as a root object of the evaluation context. 
Hence, even some HTTP headers, populated by the `DefaultHttpHeaderMapper`, are available for the evaluation context.

### Outbound Channel Adapter

The `SnsMessageHandler` is a simple one-way Outbound Channel Adapter to send Topic Notification using `SnsAsyncClient` service.

This Channel Adapter (`MessageHandler`) accepts these options:

- `topic-arn` (`topic-arn-expression`) - the SNS Topic to send notification for.
- `subject` (`subject-expression`) - the SNS Notification Subject;
- `body-expression` - the SpEL expression to evaluate the `message` property for the `software.amazon.awssdk.services.sns.model.PublishRequest`.
- `resource-id-resolver` - a `ResourceIdResolver` bean reference to resolve logical topic names to physical resource ids;

See `SnsMessageHandler` JavaDocs for more information.

The Java Config looks like:

````java
@Bean
public MessageHandler snsMessageHandler() {
    SnsMessageHandler handler = new SnsMessageHandler(amazonSns());
    adapter.setTopicArn("arn:aws:sns:eu-west:123456789012:test");
    String bodyExpression = "T(SnsBodyBuilder).withDefault(payload).forProtocols(payload.substring(0, 140), 'sms')";
    handler.setBodyExpression(spelExpressionParser.parseExpression(bodyExpression));

    // message-group ID and deduplication ID are used for FIFO topics
    handler.setMessageGroupId("foo-messages");
    String deduplicationExpression = "headers.id";
    handler.setMessageDeduplicationIdExpression(spelExpressionParser.parseExpression(deduplicationExpression))''
    return handler;
}
````

NOTE: the `bodyExpression` can be evaluated to a `org.springframework.integration.aws.support.SnsBodyBuilder` allowing the configuration of a `json` `messageStructure` for the `PublishRequest` and provide separate messages for different protocols.
The same `SnsBodyBuilder` rule is applied for the raw `payload` if the `bodyExpression` hasn't been configured.
NOTE: if the `payload` of `requestMessage` is a `software.amazon.awssdk.services.sns.model.PublishRequest` already, the `SnsMessageHandler` doesn't do anything with it, and it is sent as-is.

Starting with _version 2.0_, the `SnsMessageHandler` can be configured with the `HeaderMapper` to map message headers to the SNS message attributes.
See `SnsHeaderMapper` implementation for more information and also consult with [Amazon SNS Message Attributes][] about value types and restrictions.   

Starting with _version 2.5.3_, the `SnsMessageHandler` supports sending to SNS FIFO topics using the `messageGroupId`/`messageGroupIdExpression`
and `messageDeduplicationIdExpression` properties.

## Metadata Store for Amazon DynamoDB

The `DynamoDbMetadataStore`, a `ConcurrentMetadataStore` implementation, is provided to keep the metadata for Spring Integration components in the distributed Amazon DynamoDB store. 
The implementation is based on a simple table with `metadataKey` and `metadataValue` attributes, both are string types and the `metadataKey` is partition key of the table.
By default, the `SpringIntegrationMetadataStore` table is used, and it is created during `DynamoDbMetaDataStore` initialization if that doesn't exist yet.
The `DynamoDbMetadataStore` can be used for the `KinesisMessageDrivenChannelAdapter` as a cloud-based `cehckpointStore`.

Starting with _version 2.0_, the `DynamoDbMetadataStore` can be configured with the `timeToLive` option to enable the [DynamoDB TTL][] feature.
The `expireAt` attribute is added to each item with the value based on the sum of current time and provided `timeToLive` in seconds.
If the provided `timeToLive` value is non-positive, the TTL functionality is disabled on the table.

## Amazon Kinesis

Amazon Kinesis is a platform for streaming data on AWS, making it easy to load and analyze streaming data, and also providing the ability for you to build custom streaming data applications for specialized needs.

### Inbound Channel Adapter

The `KinesisMessageDrivenChannelAdapter` is an extension of the `MessageProducerSupport` - event-driver channel adapter.

See `KinesisMessageDrivenChannelAdapter` JavaDocs and its setters for more information how to use and how to configure it in the application for Kinesis streams ingestion.

The Java Configuration is pretty simple:

````java
@SpringBootApplication
public static class MyConfiguration {

    @Bean
    public KinesisMessageDrivenChannelAdapter kinesisInboundChannelChannel(KinesisAsyncClient amazonKinesis) {
        KinesisMessageDrivenChannelAdapter adapter =
            new KinesisMessageDrivenChannelAdapter(amazonKinesis, "MY_STREAM");
        adapter.setOutputChannel(kinesisReceiveChannel());
        return adapter;
    }
}
````

This channel adapter can be configured with the `DynamoDbMetadataStore` mentioned above to track sequence checkpoints for shards in the cloud environment when we have several instances of our Kinesis application. 
By default, this adapter uses `DeserializingConverter` to convert `byte[]` from the `Record` data.
Can be specified as `null` with meaning no conversion and the target `Message` is sent with the `byte[]` payload.

Additional headers like `AwsHeaders.RECEIVED_STREAM`, `AwsHeaders.SHARD`, `AwsHeaders.RECEIVED_PARTITION_KEY` and `AwsHeaders.RECEIVED_SEQUENCE_NUMBER` are populated to the message for downstream logic.
When `CheckpointMode.manual` is used the `Checkpointer` instance is populated to the `AwsHeaders.CHECKPOINTER` header for an acknowledgment in the downstream logic manually. 

The `KinesisMessageDrivenChannelAdapter` can be configured with the `ListenerMode` `record` or `batch` to process records one by one or send the whole just polled batch of records.
If `Converter` is configured to `null`, the entire `List<Record>` is sent as a payload.
Otherwise, a list of converted `Record.getData().array()` is wrapped to the payload of message to send.
In this case the `AwsHeaders.RECEIVED_PARTITION_KEY` and `AwsHeaders.RECEIVED_SEQUENCE_NUMBER` headers contains values as a `List<String>` of partition keys and sequence numbers of converted records respectively.

The consumer group is included to the metadata store `key`.
When records are consumed, they are filtered by the last stored `lastCheckpoint` under the key as `[CONSUMER_GROUP]:[STREAM]:[SHARD_ID]`.

Starting with _version 2.0_, the `KinesisMessageDrivenChannelAdapter` can be configured with the `InboundMessageMapper` to extract message headers embedded into the record data (if any).
See `EmbeddedJsonHeadersMessageMapper` implementation for more information.
When `InboundMessageMapper` is used together with the `ListenerMode.batch`, each `Record` is converted to the `Message` with extracted embedded headers (if any) and converted `byte[]` payload if any and converter is present.
In this case `AwsHeaders.RECEIVED_PARTITION_KEY` and `AwsHeaders.RECEIVED_SEQUENCE_NUMBER` headers are populated to the particular message for a record.
These messages are wrapped as a list payload to one outbound message. 

Starting with _version 2.0_, the `KinesisMessageDrivenChannelAdapter` can be configured with the `LockRegistry` for leader selection for the provided shards or derived from the provided streams.
The `KinesisMessageDrivenChannelAdapter` iterates over its shards and tries to acquire a distributed lock for the shard in its consumer group.
If `LockRegistry` is not provided, no exclusive locking happens and all the shards are consumed by this `KinesisMessageDrivenChannelAdapter`. 
See also `DynamoDbLockRegistry` for more information.

The `KinesisMessageDrivenChannelAdapter` can be configured with a `Function<List<Shard>, List<Shard>> shardListFilter` to filter the available, open, non-exhausted shards.
This filter `Function` will be called each time the shard list is refreshed.

For example, users may want to fully read any parent shards before starting to read their child shards.  This could be achieved as follows:

```java
    openShards -> {
        Set<String> openShardIds = openShards.stream().map(Shard::getShardId).collect(Collectors.toSet());
        // only return open shards which have no parent available for reading
        return openShards.stream()
                .filter(shard -> !openShardIds.contains(shard.getParentShardId())
                        && !openShardIds.contains(shard.getAdjacentParentShardId()))
                .collect(Collectors.toList());
        }
```

Starting with _version 3.0_, any exception thrown from the record process may lead to shard iterator rewinding to the latest check-pointed sequence or the first one in the current failed batch.
This ensures an at-least-once delivery for possibly failed records.
If the latest checkpoint is equal to the highest sequence in the batch, then shard consumer continue with the next iterator. 

Also, the `KclMessageDrivenChannelAdapter` is provided for performing streams consumption by [Kinesis Client Library][].
See its JavaDocs for more information. 

### Outbound Channel Adapter

The `KinesisMessageHandler` is an `AbstractMessageHandler` to perform put record to the Kinesis stream.
The stream, partition key (or explicit hash key) and sequence number can be determined against request message via evaluation provided expressions or can be specified statically.
They also can be specified as `AwsHeaders.STREAM`, `AwsHeaders.PARTITION_KEY` and `AwsHeaders.SEQUENCE_NUMBER` respectively.

The `KinesisMessageHandler` can be configured with the `outputChannel` for sending a `Message` on successful put operation.
The payload is the original request and additional `AwsHeaders.SHARD` and `AwsHeaders.SEQUENCE_NUMBER` headers are populated from the `PutRecordResult`. 
If the request payload is a `PutRecordsRequest`, the full `PutRecordsResult` is populated in the `AwsHeaders.SERVICE_RESULT` header instead. 

When an async failure is happened on the put operation, the `ErrorMessage` is sent to the `errorChannel` header or global one.
The payload is an `AwsRequestFailureException`.
 
The `payload` of request message can be:
 
- `PutRecordsRequest` to perform `KinesisAsyncClient.putRecords`
- `PutRecordRequest` to perform `KinesisAsyncClient.putRecord`
- `ByteBuffer` to represent a data of the `PutRecordRequest`
- `byte[]` which is wrapped to the `ByteBuffer`
- any other type which is converted to the `byte[]` by the provided `Converter`; the `SerializingConverter` is used by default.  

The Java Configuration for the message handler:

````java
@Bean
@ServiceActivator(inputChannel = "kinesisSendChannel")
public MessageHandler kinesisMessageHandler(KinesisAsyncClient amazonKinesis,
                                            MessageChannel channel) {
    KinesisMessageHandler kinesisMessageHandler = new KinesisMessageHandler(amazonKinesis);
    kinesisMessageHandler.setPartitionKey("1");
    kinesisMessageHandler.setOutputChannel(channel);
    return kinesisMessageHandler;
}
````

Starting with _version 2.0_, the `KinesisMessageHandler` can be configured with the `OutboundMessageMapper` to embed message headers into the record data alongside with the payload.
See `EmbeddedJsonHeadersMessageMapper` implementation for more information.

Also, the `KplMessageHandler` is provided for performing streams consumption by [Kinesis Producer Library][].

## Lock Registry for Amazon DynamoDB

Starting with _version 2.0_, the `DynamoDbLockRegistry` implementation is available.
Certain components (for example aggregator and resequencer) use a lock obtained from a `LockRegistry` instance to ensure that only one thread is manipulating a group at a time. 
The `DefaultLockRegistry` performs this function within a single component; you can now configure an external lock registry on these components. 
When used with a shared `MessageGroupStore`, the `DynamoDbLockRegistry` can be used to provide this functionality across multiple application instances, such that only one instance can manipulate the group at a time.   
This implementation can also be used for the distributed leader elections using a [LockRegistryLeaderInitiator][].

## Testing

The tests in the project are performed via Testcontainers and [Local Stack][] image.
See `LocalstackContainerTest` interface Javadocs for more information.

[Spring Cloud AWS]: https://awspring.io/
[AWS SDK for Java]: https://aws.amazon.com/sdkforjava/
[Amazon Web Services]: https://aws.amazon.com/
[https://aws.amazon.com/products/]: https://aws.amazon.com/products/
[https://aws.amazon.com/ses/]: https://aws.amazon.com/ses/
[https://aws.amazon.com/documentation/ses/]: https://aws.amazon.com/documentation/ses/
[FTP/FTPS Adapters Chapter]: https://docs.spring.io/spring-integration/reference/html/ftp.html
[Pull requests]: https://help.github.com/en/articles/creating-a-pull-request
[contributor guidelines]: https://github.com/spring-projects/spring-integration/blob/main/CONTRIBUTING.adoc
[administrator guidelines]: https://github.com/spring-projects/spring-integration/wiki/Administrator-Guidelines
[DynamoDB TTL]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html
[Kinesis Client Library]: https://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-kcl.html
[Amazon SQS Message Attributes]: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-attributes.html
[Amazon SNS Message Attributes]: https://docs.aws.amazon.com/sns/latest/dg/SNSMessageAttributes.html
[Leader Election]: https://docs.spring.io/spring-integration/docs/current/reference/html/messaging-endpoints-chapter.html#leadership-event-handling
[Kinesis Producer Library]: https://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-kpl.html 
[LockRegistryLeaderInitiator]: https://docs.spring.io/spring-integration/docs/current/reference/html/messaging-endpoints-chapter.html#leadership-event-handling 
[Local Stack]: https://localstack.cloud
