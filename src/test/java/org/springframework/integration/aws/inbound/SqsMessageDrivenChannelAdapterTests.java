/*
 * Copyright 2015-2023 the original author or authors.
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

package org.springframework.integration.aws.inbound;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.config.ExpressionControlBusFactoryBean;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.mock;

/**
 * @author Artem Bilan
 */
@Disabled("Revise in favor of Local Stack")
@SpringJUnitConfig
@DirtiesContext
public class SqsMessageDrivenChannelAdapterTests {

	@Autowired
	private PollableChannel inputChannel;

	@Autowired
	private SqsMessageDrivenChannelAdapter sqsMessageDrivenChannelAdapter;

	@Autowired
	private MessageChannel controlBusInput;

	@Autowired
	private PollableChannel controlBusOutput;

	@Test
	void testSqsMessageDrivenChannelAdapter() {
		assertThat(
				TestUtils.getPropertyValue(this.sqsMessageDrivenChannelAdapter, "listenerContainer.queueStopTimeout"))
				.isEqualTo(20000L);
		org.springframework.messaging.Message<?> receive = this.inputChannel.receive(1000);
		assertThat(receive).isNotNull();
		assertThat((String) receive.getPayload()).isIn("messageContent", "messageContent2");
		assertThat(receive.getHeaders().get(AwsHeaders.RECEIVED_QUEUE)).isEqualTo("testQueue");
		receive = this.inputChannel.receive(1000);
		assertThat(receive).isNotNull();
		assertThat((String) receive.getPayload()).isIn("messageContent", "messageContent2");
		assertThat(receive.getHeaders().get(AwsHeaders.RECEIVED_QUEUE)).isEqualTo("testQueue");

		try {
			this.controlBusInput.send(new GenericMessage<>("@sqsMessageDrivenChannelAdapter.stop('testQueue')"));
		}
		catch (Exception e) {
			// May fail with NPE. See
			// https://github.com/spring-cloud/spring-cloud-aws/issues/232
		}
		this.controlBusInput.send(new GenericMessage<>("@sqsMessageDrivenChannelAdapter.isRunning('testQueue')"));

		receive = this.controlBusOutput.receive(1000);
		assertThat(receive).isNotNull();
		assertThat((Boolean) receive.getPayload()).isFalse();

		this.controlBusInput.send(new GenericMessage<>("@sqsMessageDrivenChannelAdapter.start('testQueue')"));
		this.controlBusInput.send(new GenericMessage<>("@sqsMessageDrivenChannelAdapter.isRunning('testQueue')"));

		receive = this.controlBusOutput.receive(1000);
		assertThat(receive).isNotNull();
		assertThat((Boolean) receive.getPayload()).isTrue();

		assertThatThrownBy(
				() -> this.controlBusInput.send(new GenericMessage<>("@sqsMessageDrivenChannelAdapter.start('foo')")))
				.hasCauseExactlyInstanceOf(IllegalArgumentException.class)
				.hasStackTraceContaining("Queue with name 'foo' does not exist");

		assertThat(this.sqsMessageDrivenChannelAdapter.getQueues()).isEqualTo(new String[] {"testQueue"});
	}

	@Configuration
	@EnableIntegration
	public static class ContextConfiguration {

		@Bean
		public SqsAsyncClient amazonSqs() {
			SqsAsyncClient sqs = mock(SqsAsyncClient.class);
			given(sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName("testQueue").build()))
					.willReturn(CompletableFuture.completedFuture(
							GetQueueUrlResponse.builder().queueUrl("http://testQueue.amazonaws.com").build()));

			given(sqs.receiveMessage(
					ReceiveMessageRequest.builder()
							.queueUrl("http://testQueue.amazonaws.com")
							.maxNumberOfMessages(10)
							.attributeNamesWithStrings("All")
							.messageAttributeNames("All")
							.waitTimeSeconds(20)
							.build()))
					.willReturn(
							CompletableFuture.completedFuture(
									ReceiveMessageResponse.builder()
											.messages(Message.builder().body("messageContent").build(),
													Message.builder().body("messageContent2").build())
											.build()))
					.willReturn(CompletableFuture.completedFuture(ReceiveMessageResponse.builder().build()));

			given(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
					.willReturn(CompletableFuture.completedFuture(GetQueueAttributesResponse.builder().build()));

			return sqs;
		}

		@Bean
		public PollableChannel inputChannel() {
			return new QueueChannel();
		}

		@Bean
		public MessageProducer sqsMessageDrivenChannelAdapter() {
			SqsMessageDrivenChannelAdapter adapter = new SqsMessageDrivenChannelAdapter(amazonSqs(), "testQueue");
			adapter.setOutputChannel(inputChannel());
			return adapter;
		}

		@Bean
		@ServiceActivator(inputChannel = "controlBusInput")
		public ExpressionControlBusFactoryBean controlBus() {
			ExpressionControlBusFactoryBean controlBusFactoryBean = new ExpressionControlBusFactoryBean();
			controlBusFactoryBean.setOutputChannel(controlBusOutput());
			return controlBusFactoryBean;
		}

		@Bean
		public PollableChannel controlBusOutput() {
			return new QueueChannel();
		}

	}

}
