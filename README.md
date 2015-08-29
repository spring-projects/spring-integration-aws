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

For *DymamoDB* and *SimpleDB*, besides providing *Inbound*- and *Outbound Adapters*, 
a *MessageStore* implementation is provided, too.

# Executing the test cases.

All test cases for the adapters are present in the *src/test/java* folder. On executing the build, maven surefire 
plugin will execute all the tests.

> Please note that all the tests ending with **AWSTests.java* connect to the actual [Amazon Web Services][] 
and are excluded by default in the maven build. All other tests rely on mocking to test the functionality. 
You need to execute the **AWSTests.java* manually to test the connectivity to AWS using your credentials.

All these **AWSTests.java* tests look for the file *awscredentials.properties* in the classpath. 
To be on the safe side, create the following file at *src/test/resources*:
*spring-integration-aws/src/test/resources/awscredentials.properties*. It is added to the *.gitignore* file by default. 
This will prevent this file to be checked in accidentally and revealing your credentials.

This file needs to have two properties *accessKey* and *secretKey*, holding the values of your access key 
and secret key respectively.

> **Note: AWS Services are chargeable and we recommend not to execute the **AWSTests.java* as part 
of your regular builds. AWS does provide a free tier which is sufficient to perform your tests without being charged 
(not true for DynamoDB though), however keep a check on your account usage regularly. 
Get more information about AWS free tier at [http://aws.amazon.com/free/][]**

#Adapters

##Amazon Simple Storage Service (Amazon S3)

###Introduction

###Outbound Channel Adapter
###Inbound Channel Adapter

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
implementations, but Amazon SNS API is pretty simple, from other side, hence Spring Integration AWS SNS Support is 
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

[Spring Cloud AWS]: https://github.com/spring-cloud/spring-cloud-aws
[AWS SDK for Java]: http://aws.amazon.com/sdkforjava/
[Amazon Web Services]: http://aws.amazon.com/
[http://aws.amazon.com/products/]: http://aws.amazon.com/products/
[http://aws.amazon.com/ses/]: http://aws.amazon.com/ses/
[http://aws.amazon.com/documentation/ses/]: http://aws.amazon.com/documentation/ses/
[http://aws.amazon.com/free/]: http://aws.amazon.com/free/
