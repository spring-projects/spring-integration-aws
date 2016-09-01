Spring Integration Extension for Amazon Web Services (AWS)
==========================================================

# Introduction

## Amazon Web Services (AWS)

Launched in 2006, [Amazon Web Services][] (AWS) provides key infrastructure services for business through
its cloud computing platform. Using cloud computing businesses can adopt a new business model whereby they
do not have to plan and invest in procuring their own IT infrastructure. They can use the infrastructure and services
provided by the cloud service provider and pay as they use the services. Visit [http://aws.amazon.com/products/]
for more details about various products offered by Amazon as a part their cloud computing services.

*Spring Integration Extension for Amazon Web Services* provides Spring Integration adapters for the various services
provided by the [AWS SDK for Java][].
Note the Spring Integration AWS Extension is based on the [Spring Cloud AWS][] project.

## Spring Integration's extensions to AWS

This guide intends to explain briefly the various adapters available for [Amazon Web Services][] such as:

* **Amazon Simple Email Service (SES)**
* **Amazon Simple Storage Service (S3)**
* **Amazon Simple Queue Service (SQS)**
* **Amazon Simple Notification Service (SNS)**
* **Amazon DynamoDB** (Analysis ongoing)
* **Amazon SimpleDB** (Not initiated)

Sample XML Namespace configurations for each adapter as well as sample code snippets are provided wherever necessary.
Of the above libraries, *SES* and *SNS* provide outbound adapters only. All other services have inbound
and outbound adapters. The *SQS* inbound adapter is capable of receiving notifications sent out from *SNS*
where the topic is an *SQS* Queue.

## Contributing

[Pull requests][] are welcome. Please see the [contributor guidelines][] for details. Additionally, if you are contributing, we recommend following the process for Spring Integration as outlined in the [administrator guidelines][].

#Adapters

##Amazon Simple Storage Service (Amazon S3)

###Introduction

The S3 Channel Adapters are based on the `AmazonS3` template and `TransferManager`.
See their specification and JavaDocs for more information.

###Inbound Channel Adapter

The S3 Inbound Channel Adapter is represented by the `S3InboundFileSynchronizingMessageSource`
 (`<int-aws:s3-inbound-channel-adapter>`) and allows to pull S3 objects as files from the S3 bucket
to the local directory for synchronization.
This adapter is fully similar to the Inbound Channel Adapters in the FTP and SFTP Spring Integration modules.
See more information in the [FTP/FTPS Adapters Chapter][] for common options or `SessionFactory`, `RemoteFileTemplate`
and `FileListFilter` abstractions.

The Java Configuration is:

````java
@SpringBootApplication
public static class MyConfiguration {

    @Autowired
    private AmazonS3 amazonS3;

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

With this config you receive messages with `java.io.File` `payload` from the `s3FilesChannel`
after periodic synchronization of content from the Amazon S3 bucket into the local directory.

An XML variant may look like:

````xml
<bean id="s3SessionFactory" class="org.springframework.integration.aws.support.S3SessionFactory"/>

<int-aws:s3-inbound-channel-adapter channel="s3Channel"
                   session-factory="s3SessionFactory"
                   auto-create-local-directory="true"
                   delete-remote-files="true"
                   preserve-timestamp="true"
                   filename-pattern="*.txt"
                   local-directory="."
                   local-filename-generator-expression="#this.toUpperCase() + '.a' + @fooString"
                   temporary-file-suffix=".foo"
                   local-filter="acceptAllFilter"
                   remote-directory-expression="'my_bucket'">
    <int:poller fixed-rate="1000"/>
</int-aws:s3-inbound-channel-adapter>
````

###Streaming Inbound Channel Adapter

This adapter produces message with payloads of type `InputStream`, allowing S3 objects to be fetched without writing to the local file system. 
Since the session remains open, the consuming application is responsible for closing the session when the file has been consumed. 
The session is provided in the closeableResource header (`IntegrationMessageHeaderAccessor.CLOSEABLE_RESOURCE`). Standard framework components, such as the `FileSplitter` and `StreamTransformer` will automatically close the session.
 
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
    private AmazonS3 amazonS3;

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

An XML variant may look like:

````xml
<bean id="metadataStore" class="org.springframework.integration.metadata.SimpleMetadataStore"/>

<bean id="acceptOnceFilter" class="org.springframework.integration.aws.support.filters.S3PersistentAcceptOnceFileListFilter">
	<constructor-arg index="0" ref="metadataStore"/>
	<constructor-arg index="1" value="streaming"/>
</bean>

<bean id="s3SessionFactory" class="org.springframework.integration.aws.support.S3SessionFactory"/>

<int-aws:s3-inbound-streaming-channel-adapter channel="s3Channel"
                   session-factory="s3SessionFactory"
                   filter="acceptOnceFilter"
                   remote-directory-expression="'my_bucket'">
    <int:poller fixed-rate="1000"/>
</int-aws:s3-inbound-streaming-channel-adapter>
````

Only one of `filename-pattern`, `filename-regex` or `filter` is allowed.

> NOTE: Unlike the non-streaming inbound channel adapter, this adapter does not prevent duplicates by default. 
> If you do not delete the remote file and you wish to prevent the file being processed again, you can configure an `S3PersistentFileListFilter` in the `filter` attribute. 
> If you don’t actually want to persist the state, an in-memory `SimpleMetadataStore` can be used with the filter. 
> If you wish to use a filename pattern (or regex) as well, use a `CompositeFileListFilter`.

###Outbound Channel Adapter

The S3 Outbound Channel Adapter is represented by the `S3MessageHandler` (`<int-aws:s3-outbound-channel-adapter>`
and `<int-aws:s3-outbound-gateway>`) and allows to perform `upload`, `download` and `copy`
(see `S3MessageHandler.Command` enum) operations in the provided S3 bucket.

The Java Configuration is:

````java
@SpringBootApplication
public static class MyConfiguration {

    @Autowired
    private AmazonS3 amazonS3;

    @Bean
    @ServiceActivator(inputChannel = "s3UploadChannel")
    public MessageHandler s3MessageHandler() {
    	return new S3MessageHandler(amazonS3(), "myBuck");
    }

}
````

With this config you can send message with the `java.io.File` as `payload` and the `transferManager.upload()`
operation will be performed, where the file name is used as a S3 Object key.

An XML variant may look like:

````xml
<bean id="transferManager" class="com.amazonaws.services.s3.transfer.TransferManager"/>

<int-aws:s3-outbound-channel-adapter transfer-manager="transferManager"
									  channel="s3SendChannel"
									  bucket="foo"
									  command="DOWNLOAD"
									  key="myDirectory"/>
````

See more information in the `S3MessageHandler` JavaDocs and `<int-aws:s3-outbound-channel-adapter>` &
`<int-aws:s3-outbound-gateway>` descriptions.

###Outbound Gateway

The S3 Outbound Gateway is represented by the same `S3MessageHandler` with the `produceReply = true` constructor
 argument for Java Configuration and `<int-aws:s3-outbound-gateway>` for xml definitions.

The "request-reply" nature of this gateway is async and the `Transfer` result from the `TransferManager`
operation is sent to the `outputChannel`, assuming the transfer progress observation in the downstream flow.

The `S3ProgressListener can be supplied to track the transfer progress.
Also the listener can be populated into the returned `Transfer` afterwards in the downstream flow.

See more information in the `S3MessageHandler` JavaDocs and `<int-aws:s3-outbound-channel-adapter>` &
`<int-aws:s3-outbound-gateway>` descriptions.

##Simple Email Service (SES)

There is no adapter for SES, since [Spring Cloud AWS][] provides implementations for
`org.springframework.mail.MailSender` - `SimpleEmailServiceMailSender` and `SimpleEmailServiceJavaMailSender`, which
can be injected to the `<int-mail:outbound-channel-adapter>`.

##Amazon Simple Queue Service (SQS)

The `SQS` adapters are fully based on the [Spring Cloud AWS][] foundation, so for more information about the
background components and core configuration, please, refer to the documentation of that project.

###Outbound Channel Adapter

The SQS Outbound Channel Adapter is presented by the `SqsMessageHandler` implementation
(`<int-aws:sqs-outbound-channel-adapter>`) and allows to send message to the SQS `queue` with provided `AmazonSQS`
client. An SQS queue can be configured explicitly on the adapter (using
`org.springframework.integration.expression.ValueExpression`) or as a SpEL `Expression`, which is evaluated against
request message as a root object of evaluation context. In addition the `queue` can be extracted from the message
headers under `AwsHeaders.QUEUE`.

The Java Configuration is pretty simple:

````java
@SpringBootApplication
public static class MyConfiguration {

    @Autowired
    private AmazonSQS amazonSqs;

    @Bean
    public QueueMessagingTemplate queueMessagingTemplate() {
    	return new QueueMessagingTemplate(this.amazonSqs);
    }

    @Bean
    @ServiceActivator(inputChannel = "sqsSendChannel")
    public MessageHandler sqsMessageHandler() {
    	return new SqsMessageHandler(queueMessagingTemplate());
    }

}
````

An XML variant may look like:

````xml
<aws-messaging:sqs-async-client id="sqs"/>

<int-aws:sqs-outbound-channel-adapter sqs="sqs"
									  channel="sqsSendChannel"
									  queue="foo"/>
````

###Inbound Channel Adapter

The SQS Inbound Channel Adapter is a `message-driven` implementation for the `MessageProducer` and is represented with
`SqsMessageDrivenChannelAdapter`. This channel adapter is based on the
`org.springframework.cloud.aws.messaging.listener.SimpleMessageListenerContainer` to receive messages from the
provided `queues` in async manner and send an enhanced Spring Integration Message to the provided `MessageChannel`.
The enhancements includes `AwsHeaders.MESSAGE_ID`, `AwsHeaders.RECEIPT_HANDLE` and `AwsHeaders.QUEUE` message headers.

The Java Configuration is pretty simple:

````java
@SpringBootApplication
public static class MyConfiguration {

	@Autowired
	private AmazonSQSAsync amazonSqs;

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

An XML variant may look like:

````xml
<aws-messaging:sqs-async-client id="sqs"/>

<int-aws:sqs-message-driven-channel-adapter id="sqsInboundChannel"
                                          sqs="sqs"
										  error-channel="myErrorChannel"
										  queues="foo, bar"
										  delete-message-on-exception="false"
										  max-number-of-messages="5"
										  visibility-timeout="200"
										  wait-time-out="40"
										  send-timeout="2000"/>
````

The `SqsMessageDrivenChannelAdapter` exposes all `SimpleMessageListenerContainer` attributes to configure and one an
important of them is `deleteMessageOnException`, which is `true` by default. Having that to `false`, it is a
responsibility of end-application to delete message or not on exceptions. E.g. in the error flow on the
`error-channel` of this channel adapter. For this purpose a `AwsHeaders.RECEIPT_HANDLE` message header must be used
for the message deletion:

````java
MessageHeaders headers = message.getHeaders();
this.amazonSqs.deleteMessageAsync(
          new DeleteMessageRequest(headers.get(AwsHeaders.QUEUE), headers.get(AwsHeaders.RECEIPT_HANDLE)));
````

##Amazon Simple Notification Service (SNS)

Amazon SNS is a publish-subscribe messaging system that allows clients to publish notification to a particular topic.
Other interested clients may subscribe using different protocols like HTTP/HTTPS, e-mail or an Amazon SQS queue to
receive the messages. Plus mobile devices can be registered as subscribers from the AWS Management Console.

Unfortunately [Spring Cloud AWS][] doesn't provide flexible components which can be used from the channel adapter
implementations, but Amazon SNS API is pretty simple, on the other hand. Hence Spring Integration AWS SNS Support is
straightforward and just allows to provide channel adapter foundation for Spring Integration applications.

Since e-mail, SMS and mobile devices subscription/unsubscription confirmation is out of the Spring Integration
application scope and can be done only from the AWS Management Console, we provide only HTTP/HTTPS SNS endpoint in
face of `SnsInboundChannelAdapter`. The SQS-to-SNS subscription can be done with the simple usage of
`com.amazonaws.services.sns.util.Topics#subscribeQueue()`, which confirms subscription automatically.

###Inbound Channel Adapter

The `SnsInboundChannelAdapter` (`<int-aws:sns-inbound-channel-adapter>`) is an extension of
`HttpRequestHandlingMessagingGateway` and must be as a part of Spring MVC application. Its URL must be used from the
AWS Management Console to add this endpoint as a subscriber to the SNS Topic. However before receiving any
notification itself this HTTP endpoint must confirm the subscription.

See `SnsInboundChannelAdapter` JavaDocs for more information.

An important option of this adapter to consider is `handleNotificationStatus`. This `boolean` flag indicates if the
adapter should send `SubscriptionConfirmation/UnsubscribeConfirmation` message to the `output-channel` or not. If
that the `AwsHeaders.NOTIFICATION_STATUS` message header is present in the message with the `NotificationStatus`
object, which can be used in the downstream flow to confirm subscription or not. Or "re-confirm" it in case of
`UnsubscribeConfirmation` message.

In addition the `AwsHeaders#SNS_MESSAGE_TYPE` message header is represent to simplify a routing in the downstream flow.

The Java Configuration is pretty simple:

````java
@SpringBootApplication
public static class MyConfiguration {

	@Autowired
	private AmazonSNS amazonSns;

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

An XML variant may look like:

````xml
<int-aws:sns-inbound-channel-adapter sns="amazonSns"
										 path="/foo"
										 channel="snsChannel"
										 error-channel="errorChannel"
										 handle-notification-status="true"
										 payload-expression="payload.Message"/>
```

Note: by default the message `payload` is a `Map` converted from the received Topic JSON message. For the convenient
the `payload-expression` is provided with the `Message` as a root object of the evaluation context. Hence even some
HTTP headers, populated by the `DefaultHttpHeaderMapper`, are available for the evaluation context.

###Outbound Channel Adapter

The `SnsMessageHandler` (`<int-aws:sns-outbound-channel-adapter>`) is a simple one-way Outbound Channel Adapter
to send Topic Notification using `AmazonSNS` service.

This Channel Adapter (`MessageHandler`) accepts these options:

- `topic-arn` (`topic-arn-expression`) - the SNS Topic to send notification for.
- `subject` (`subject-expression`) - the SNS Notification Subject;
- `body-expression` - the SpEL expression to evaluate the `message` property for the
`com.amazonaws.services.sns.model.PublishRequest`.
- `resource-id-resolver` - a `ResourceIdResolver` bean reference to resolve logical topic names to physical resource ids;

See `SnsMessageHandler` JavaDocs for more information.

The Java Config looks like:

````java
@Bean
public MessageHandler snsMessageHandler() {
    SnsMessageHandler handler = new SnsMessageHandler(amazonSns());
    adapter.setTopicArn("arn:aws:sns:eu-west:123456789012:test);
    String bodyExpression = "SnsBodyBuilder.withDefault(payload).forProtocols(payload.substring(0, 140), 'sms')";
    handler.setBodyExpression(spelExpressionParser.parseExpression(bodyExpression));
    return handler;
}
````

NOTE: the `bodyExpression` can be evaluated to a `org.springframework.integration.aws.support.SnsBodyBuilder`
allowing the configuration of a `json` `messageStructure` for the `PublishRequest` and provide separate messages
for different protocols.
The same `SnsBodyBuilder` rule is applied for the raw `payload` if the `bodyExpression` hasn't been configured.
NOTE: if the `payload` of `requestMessage` is a `com.amazonaws.services.sns.model.PublishRequest` already,
the `SnsMessageHandler` doesn't do anything with it and it is sent as-is.

The XML variant may look like:

````xml
<int-aws:sns-outbound-channel-adapter
			id="snsAdapter"
			sns="amazonSns"
			channel="notificationChannel"
			topic-arn="foo"
			subject="bar"
			body-expression="payload.toUpperCase()"/>
````

###Outbound Gateway

The `<int-aws:sns-outbound-gateway>` is fully similar to the one-way channel adapter.
The only difference that in gateway mode the `SnsMessageHandler` produces the reply `Message` as:

````java
return getMessageBuilderFactory()
           .withPayload(publishRequest)
           .setHeader(AwsHeaders.TOPIC, publishRequest.getTopicArn())
           .setHeader(AwsHeaders.SNS_PUBLISHED_MESSAGE_ID, publishResult.getMessageId());
````

The reply from the `<int-aws:sns-outbound-gateway>` may be useful to track and correlate the SNS message
in the downstream flow.

With Java configuration there is just enough to use constructor of `SnsMessageHandler` with `produceReply` boolean
flag to `true` ot switch it to the gateway mode.
By default the `SnsMessageHandler` is one-way `MessageHandler`.


[Spring Cloud AWS]: https://github.com/spring-cloud/spring-cloud-aws
[AWS SDK for Java]: http://aws.amazon.com/sdkforjava/
[Amazon Web Services]: http://aws.amazon.com/
[http://aws.amazon.com/products/]: http://aws.amazon.com/products/
[http://aws.amazon.com/ses/]: http://aws.amazon.com/ses/
[http://aws.amazon.com/documentation/ses/]: http://aws.amazon.com/documentation/ses/
[Reference Manual]: http://docs.spring.io/spring-integration/reference/html/ftp.html
[Pull requests]: http://help.github.com/send-pull-requests
[contributor guidelines]: https://github.com/spring-projects/spring-integration/blob/master/CONTRIBUTING.adoc
