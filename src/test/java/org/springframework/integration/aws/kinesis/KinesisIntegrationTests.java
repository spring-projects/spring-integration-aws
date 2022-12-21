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

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.aws.LocalstackContainerTest;
import org.springframework.integration.aws.inbound.kinesis.KinesisMessageDrivenChannelAdapter;
import org.springframework.integration.aws.inbound.kinesis.KinesisMessageHeaderErrorMessageStrategy;
import org.springframework.integration.aws.outbound.KinesisMessageHandler;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.metadata.SimpleMetadataStore;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.support.json.EmbeddedJsonHeadersMessageMapper;
import org.springframework.integration.support.locks.DefaultLockRegistry;
import org.springframework.integration.support.locks.LockRegistry;
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
@SpringJUnitConfig
@DirtiesContext
public class KinesisIntegrationTests implements LocalstackContainerTest {

	private static final String TEST_STREAM = "TestStream";

	private static AmazonKinesisAsync AMAZON_KINESIS_ASYNC;

	@Autowired
	private MessageChannel kinesisSendChannel;

	@Autowired
	private PollableChannel kinesisReceiveChannel;

	@Autowired
	private PollableChannel errorChannel;

	@BeforeAll
	static void setup() throws Exception {
		AMAZON_KINESIS_ASYNC = LocalstackContainerTest.kinesisClient();
		AMAZON_KINESIS_ASYNC.createStream(TEST_STREAM, 1);

		int n = 0;
		while (n++ < 100 && !"ACTIVE".equals(
				AMAZON_KINESIS_ASYNC.describeStream(TEST_STREAM).getStreamDescription().getStreamStatus())) {

			Thread.sleep(200);
		}
	}

	@AfterAll
	static void tearDown() {
		AMAZON_KINESIS_ASYNC.deleteStream(TEST_STREAM);
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

		for (int i = 0; i < 2; i++) {
			this.kinesisSendChannel
					.send(MessageBuilder.withPayload(new Date()).setHeader(AwsHeaders.STREAM, TEST_STREAM).build());
		}

		Set<String> receivedSequences = new HashSet<>();

		for (int i = 0; i < 2; i++) {
			receive = this.kinesisReceiveChannel.receive(30_000);
			assertThat(receive).isNotNull();
			String sequenceNumber = receive.getHeaders().get(AwsHeaders.RECEIVED_SEQUENCE_NUMBER, String.class);
			assertThat(receivedSequences.add(sequenceNumber)).isTrue();
		}

		assertThat(receivedSequences.size()).isEqualTo(2);

		receive = this.kinesisReceiveChannel.receive(10);
		assertThat(receive).isNull();
	}

	@Configuration
	@EnableIntegration
	public static class TestConfiguration {

		@Bean
		@ServiceActivator(inputChannel = "kinesisSendChannel")
		public MessageHandler kinesisMessageHandler() {
			KinesisMessageHandler kinesisMessageHandler = new KinesisMessageHandler(AMAZON_KINESIS_ASYNC);
			kinesisMessageHandler.setPartitionKey("1");
			kinesisMessageHandler.setEmbeddedHeadersMapper(new EmbeddedJsonHeadersMessageMapper("foo"));
			return kinesisMessageHandler;
		}

		@Bean
		public ConcurrentMetadataStore checkpointStore() {
			return new SimpleMetadataStore();
		}

		@Bean
		public LockRegistry lockRegistry() {
			return new DefaultLockRegistry();
		}

		private KinesisMessageDrivenChannelAdapter kinesisMessageDrivenChannelAdapter() {
			KinesisMessageDrivenChannelAdapter adapter = new KinesisMessageDrivenChannelAdapter(
					AMAZON_KINESIS_ASYNC, TEST_STREAM);
			adapter.setOutputChannel(kinesisReceiveChannel());
			adapter.setErrorChannel(errorChannel());
			adapter.setErrorMessageStrategy(new KinesisMessageHeaderErrorMessageStrategy());
			adapter.setCheckpointStore(checkpointStore());
			adapter.setLockRegistry(lockRegistry());
			adapter.setEmbeddedHeadersMapper(new EmbeddedJsonHeadersMessageMapper("foo"));
			adapter.setBindSourceRecord(true);

			DirectFieldAccessor dfa = new DirectFieldAccessor(adapter);
			dfa.setPropertyValue("describeStreamBackoff", 10);
			dfa.setPropertyValue("consumerBackoff", 10);
			dfa.setPropertyValue("idleBetweenPolls", 1);

			return adapter;
		}

		@Bean
		public KinesisMessageDrivenChannelAdapter kinesisInboundChannelChannel1() {
			return kinesisMessageDrivenChannelAdapter();
		}

		@Bean
		public KinesisMessageDrivenChannelAdapter kinesisInboundChannelChannel2() {
			return kinesisMessageDrivenChannelAdapter();
		}

		@Bean
		public KinesisMessageDrivenChannelAdapter kinesisInboundChannelChannel3() {
			return kinesisMessageDrivenChannelAdapter();
		}

		@Bean
		public KinesisMessageDrivenChannelAdapter kinesisInboundChannelChannel4() {
			return kinesisMessageDrivenChannelAdapter();
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
