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
* **Amazon Simple Queue Service (SQS)** (Development complete, coming soon)
* **Amazon DynamoDB** (Analysis ongoing)
* **Amazon SimpleDB** (Not initiated)
* **Amazon SNS** (Not initiated)

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

[Spring Cloud AWS]: https://github.com/spring-cloud/spring-cloud-aws
[AWS SDK for Java]: http://aws.amazon.com/sdkforjava/
[Amazon Web Services]: http://aws.amazon.com/
[http://aws.amazon.com/products/]: http://aws.amazon.com/products/
[http://aws.amazon.com/ses/]: http://aws.amazon.com/ses/
[http://aws.amazon.com/documentation/ses/]: http://aws.amazon.com/documentation/ses/
[http://aws.amazon.com/free/]: http://aws.amazon.com/free/
