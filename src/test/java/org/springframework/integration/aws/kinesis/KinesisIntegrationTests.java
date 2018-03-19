/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.aws.kinesis;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.aws.KinesisLocalRunning;
import org.springframework.integration.aws.inbound.kinesis.KinesisMessageDrivenChannelAdapter;
import org.springframework.integration.aws.inbound.kinesis.KinesisMessageHeaderErrorMessageStrategy;
import org.springframework.integration.aws.outbound.KinesisMessageHandler;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.metadata.SimpleMetadataStore;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Artem Bilan
 *
 * @since 1.1
 */
@RunWith(SpringRunner.class)
@DirtiesContext
public class KinesisIntegrationTests {

	@ClassRule
	public static final KinesisLocalRunning KINESIS_LOCAL_RUNNING = KinesisLocalRunning.isRunning(4567);

	private static final String TEST_STREAM = "TestStream";

	@Autowired
	private MessageChannel kinesisSendChannel;

	@Autowired
	private PollableChannel kinesisReceiveChannel;

	@Autowired
	private PollableChannel errorChannel;

	@BeforeClass
	public static void setup() {
		KINESIS_LOCAL_RUNNING.getKinesis().createStream(TEST_STREAM, 1);
	}

	@AfterClass
	public static void tearDown() {
		KINESIS_LOCAL_RUNNING.getKinesis().deleteStream(TEST_STREAM);
	}

	@Test
	public void testKinesisInboundOutbound() throws InterruptedException {
		this.kinesisSendChannel.send(
				MessageBuilder.withPayload("foo")
						.setHeader(AwsHeaders.STREAM, TEST_STREAM)
						.build());

		Date now = new Date();
		this.kinesisSendChannel.send(
				MessageBuilder.withPayload(now)
						.setHeader(AwsHeaders.STREAM, TEST_STREAM)
						.build());

		Message<?> receive = this.kinesisReceiveChannel.receive(10_000);
		assertThat(receive).isNotNull();
		assertThat(receive.getPayload()).isEqualTo(now);

		Message<?> errorMessage = this.errorChannel.receive(10_000);
		assertThat(errorMessage).isNotNull();
		assertThat(errorMessage.getHeaders().get(AwsHeaders.RAW_RECORD)).isNotNull();
		assertThat(((Exception) errorMessage.getPayload()).getMessage())
				.contains("Channel 'kinesisReceiveChannel' expected one of the following datataypes " +
						"[class java.util.Date], but received [class java.lang.String]");


		for (int i = 0; i < 1000; i++) {
			this.kinesisSendChannel.send(
					MessageBuilder.withPayload(new Date())
							.setHeader(AwsHeaders.STREAM, TEST_STREAM)
							.build());
		}

		Set<String> receivedSequences = new HashSet<>();


		for (int i = 0; i < 1000; i++) {
			receive = this.kinesisReceiveChannel.receive(10_000);
			assertThat(receive).isNotNull();
			String sequenceNumber = receive.getHeaders().get(AwsHeaders.RECEIVED_SEQUENCE_NUMBER, String.class);
			assertThat(receivedSequences.add(sequenceNumber)).isTrue();
		}

		assertThat(receivedSequences.size()).isEqualTo(1000);

		receive = this.kinesisReceiveChannel.receive(10);
		assertThat(receive).isNull();
	}

	@Configuration
	@EnableIntegration
	public static class TestConfiguration {

		@Bean
		@ServiceActivator(inputChannel = "kinesisSendChannel")
		public MessageHandler kinesisMessageHandler() {
			KinesisMessageHandler kinesisMessageHandler = new KinesisMessageHandler(KINESIS_LOCAL_RUNNING.getKinesis());
			kinesisMessageHandler.setPartitionKey("1");
			return kinesisMessageHandler;
		}

		@Bean
		public ConcurrentMetadataStore checkpointStore() {
			return new SimpleMetadataStore();
		}

		private KinesisMessageDrivenChannelAdapter kinesisMessageDrivenChannelAdapter() {
			KinesisMessageDrivenChannelAdapter adapter =
					new KinesisMessageDrivenChannelAdapter(KINESIS_LOCAL_RUNNING.getKinesis(), TEST_STREAM);
			adapter.setOutputChannel(kinesisReceiveChannel());
			adapter.setErrorChannel(errorChannel());
			adapter.setErrorMessageStrategy(new KinesisMessageHeaderErrorMessageStrategy());
			adapter.setCheckpointStore(checkpointStore());
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
			queueChannel.addInterceptor(new ChannelInterceptorAdapter() {

				@Override
				public void postSend(Message<?> message, MessageChannel channel, boolean sent) {
					super.postSend(message, channel, sent);

					if (message instanceof ErrorMessage) {
						throw (RuntimeException) ((ErrorMessage) message).getPayload();
					}
				}
			});
			return queueChannel;
		}

	}

}
