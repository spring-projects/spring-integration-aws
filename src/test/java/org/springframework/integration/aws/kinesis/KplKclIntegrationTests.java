/*
 * Copyright 2017-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.aws.kinesis;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.aws.LocalstackContainerTest;
import org.springframework.integration.aws.inbound.kinesis.KclMessageDrivenChannelAdapter;
import org.springframework.integration.aws.inbound.kinesis.KinesisMessageHeaderErrorMessageStrategy;
import org.springframework.integration.aws.outbound.KplMessageHandler;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.support.json.EmbeddedJsonHeadersMessageMapper;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * @author Artem Bilan
 *
 * @since 1.1
 */
@Disabled("Depends on real call to http://169.254.169.254 through native library")
@SpringJUnitConfig
@DirtiesContext
public class KplKclIntegrationTests implements LocalstackContainerTest {

	private static final String TEST_STREAM = "TestStreamKplKcl";

	private static AmazonKinesis AMAZON_KINESIS;

	private static AmazonDynamoDB DYNAMO_DB;

	private static AmazonCloudWatch CLOUD_WATCH;

	@Autowired
	private MessageChannel kinesisSendChannel;

	@Autowired
	private PollableChannel kinesisReceiveChannel;

	@Autowired
	private PollableChannel errorChannel;

	@BeforeAll
	static void setup() throws InterruptedException {
		AMAZON_KINESIS = LocalstackContainerTest.kinesisClient();
		DYNAMO_DB = LocalstackContainerTest.dynamoDbClient();
		CLOUD_WATCH = LocalstackContainerTest.cloudWatchClient();
		AMAZON_KINESIS.createStream(TEST_STREAM, 1);

		int n = 0;
		while (n++ < 100 && !"ACTIVE".equals(
				AMAZON_KINESIS.describeStream(TEST_STREAM).getStreamDescription().getStreamStatus())) {

			Thread.sleep(200);
		}
	}

	@AfterAll
	static void tearDown() {
		AMAZON_KINESIS.deleteStream(TEST_STREAM);
	}

	@Test
	void testKinesisInboundOutbound() {
		this.kinesisSendChannel
				.send(MessageBuilder.withPayload("foo").setHeader(AwsHeaders.STREAM, TEST_STREAM).build());

		Date now = new Date();
		this.kinesisSendChannel.send(MessageBuilder.withPayload(now).setHeader(AwsHeaders.STREAM, TEST_STREAM)
				.setHeader("foo", "BAR").build());

		Message<?> receive = this.kinesisReceiveChannel.receive(30_000);
		assertThat(receive).isNotNull();
		assertThat(receive.getPayload()).isEqualTo(now);
		assertThat(receive.getHeaders()).contains(entry("foo", "BAR"));
		assertThat(receive.getHeaders()).containsKey(IntegrationMessageHeaderAccessor.SOURCE_DATA);

		Message<?> errorMessage = this.errorChannel.receive(30_000);
		assertThat(errorMessage).isNotNull();
		assertThat(errorMessage.getHeaders().get(AwsHeaders.RAW_RECORD)).isNotNull();
		assertThat(((Exception) errorMessage.getPayload()).getMessage())
				.contains("Channel 'kinesisReceiveChannel' expected one of the following data types "
						+ "[class java.util.Date], but received [class java.lang.String]");

		this.kinesisSendChannel
				.send(MessageBuilder.withPayload(new Date()).setHeader(AwsHeaders.STREAM, TEST_STREAM).build());

		receive = this.kinesisReceiveChannel.receive(30_000);
		assertThat(receive).isNotNull();
		assertThat(receive.getHeaders().get(AwsHeaders.RECEIVED_SEQUENCE_NUMBER, String.class)).isNotEmpty();

		receive = this.kinesisReceiveChannel.receive(10);
		assertThat(receive).isNull();
	}

	@Configuration
	@EnableIntegration
	public static class TestConfiguration {

		@Bean
		public KinesisProducerConfiguration kinesisProducerConfiguration() throws URISyntaxException {
			URI kinesisUri =
					LocalstackContainerTest.LOCAL_STACK_CONTAINER.getEndpointOverride(LocalStackContainer.Service.KINESIS);
			URI cloudWatchUri =
					LocalstackContainerTest.LOCAL_STACK_CONTAINER.getEndpointOverride(LocalStackContainer.Service.CLOUDWATCH);

			return new KinesisProducerConfiguration()
					.setCredentialsProvider(LocalstackContainerTest.credentialsProvider())
					.setRegion(LocalstackContainerTest.LOCAL_STACK_CONTAINER.getRegion())
					.setKinesisEndpoint(kinesisUri.getHost())
					.setKinesisPort(kinesisUri.getPort())
					.setCloudwatchEndpoint(cloudWatchUri.getHost())
					.setCloudwatchPort(cloudWatchUri.getPort())
					.setVerifyCertificate(false);
		}

		@Bean
		@ServiceActivator(inputChannel = "kinesisSendChannel")
		public MessageHandler kplMessageHandler(KinesisProducerConfiguration kinesisProducerConfiguration) {
			KplMessageHandler kinesisMessageHandler =
					new KplMessageHandler(new KinesisProducer(kinesisProducerConfiguration));
			kinesisMessageHandler.setPartitionKey("1");
			kinesisMessageHandler.setEmbeddedHeadersMapper(new EmbeddedJsonHeadersMessageMapper("foo"));
			return kinesisMessageHandler;
		}

		@Bean
		public KclMessageDrivenChannelAdapter kclMessageDrivenChannelAdapter() {
			KclMessageDrivenChannelAdapter adapter =
					new KclMessageDrivenChannelAdapter(
							TEST_STREAM, AMAZON_KINESIS, CLOUD_WATCH, DYNAMO_DB,
							LocalstackContainerTest.credentialsProvider());
			adapter.setOutputChannel(kinesisReceiveChannel());
			adapter.setErrorChannel(errorChannel());
			adapter.setErrorMessageStrategy(new KinesisMessageHeaderErrorMessageStrategy());
			adapter.setEmbeddedHeadersMapper(new EmbeddedJsonHeadersMessageMapper("foo"));
			adapter.setStreamInitialSequence(InitialPositionInStream.TRIM_HORIZON);
			adapter.setBindSourceRecord(true);
			return adapter;
		}

		@Bean
		public PollableChannel kinesisReceiveChannel() {
			QueueChannel queueChannel = new QueueChannel();
			queueChannel.setDatatypes(Date.class);
			return queueChannel;
		}

		@Bean
		public PollableChannel errorChannel() {
			QueueChannel queueChannel = new QueueChannel();
			queueChannel.addInterceptor(new ChannelInterceptor() {

				@Override
				public void postSend(Message<?> message, MessageChannel channel, boolean sent) {
					if (message instanceof ErrorMessage) {
						throw (RuntimeException) ((ErrorMessage) message).getPayload();
					}
				}

			});
			return queueChannel;
		}

	}

}
