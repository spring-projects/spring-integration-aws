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

package org.springframework.integration.aws.outbound;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.awspring.cloud.sqs.listener.QueueNotFoundStrategy;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Instantiating SqsMessageHandler using amazonSqs.
 *
 * @author Artem Bilan
 * @author Rahul Pilani
 * @author Seth Kelly
 */
@Disabled("Revise in favor of Local Stack")
@SpringJUnitConfig
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class SqsMessageHandlerTests {

	@Autowired
	protected SqsAsyncClient amazonSqs;

	@Autowired
	protected MessageChannel sqsSendChannel;

	@Autowired
	protected MessageChannel sqsSendChannelWithAutoCreate;

	@Autowired
	protected SqsMessageHandler sqsMessageHandler;

	@Autowired
	protected SqsMessageHandler sqsMessageHandlerWithQueueAutoCreate;

	@Test
	void testSqsMessageHandler() {
		final Message<String> message = MessageBuilder.withPayload("message").build();

		assertThatExceptionOfType(MessageHandlingException.class)
				.isThrownBy(() -> this.sqsSendChannel.send(message))
				.withCauseInstanceOf(IllegalStateException.class);

		this.sqsMessageHandler.setQueue("foo");
		this.sqsSendChannel.send(message);
		ArgumentCaptor<SendMessageRequest> sendMessageRequestArgumentCaptor =
				ArgumentCaptor.forClass(SendMessageRequest.class);
		verify(this.amazonSqs).sendMessage(sendMessageRequestArgumentCaptor.capture());
		assertThat(sendMessageRequestArgumentCaptor.getValue().queueUrl()).isEqualTo("https://queue-url.com/foo");

		Message<String> message2 = MessageBuilder.withPayload("message").setHeader(AwsHeaders.QUEUE, "bar").build();
		this.sqsSendChannel.send(message2);
		verify(this.amazonSqs, times(2)).sendMessage(sendMessageRequestArgumentCaptor.capture());

		assertThat(sendMessageRequestArgumentCaptor.getValue().queueUrl()).isEqualTo("https://queue-url.com/bar");

		SpelExpressionParser spelExpressionParser = new SpelExpressionParser();
		Expression expression = spelExpressionParser.parseExpression("headers.foo");
		this.sqsMessageHandler.setQueueExpression(expression);
		message2 = MessageBuilder.withPayload("message").setHeader("foo", "baz").build();
		this.sqsSendChannel.send(message2);
		verify(this.amazonSqs, times(3)).sendMessage(sendMessageRequestArgumentCaptor.capture());

		SendMessageRequest sendMessageRequestArgumentCaptorValue = sendMessageRequestArgumentCaptor.getValue();
		assertThat(sendMessageRequestArgumentCaptorValue.queueUrl()).isEqualTo("https://queue-url.com/baz");

		Map<String, MessageAttributeValue> messageAttributes = sendMessageRequestArgumentCaptorValue.messageAttributes();

		assertThat(messageAttributes).doesNotContainKey(MessageHeaders.ID);
		assertThat(messageAttributes).doesNotContainKey(MessageHeaders.TIMESTAMP);
		assertThat(messageAttributes).containsKey("foo");
		assertThat(messageAttributes.get("foo").stringValue()).isEqualTo("baz");
	}

	@Test
	@SuppressWarnings("unchecked")
	void testSqsMessageHandlerWithAutoQueueCreate() {
		Message<String> message = MessageBuilder.withPayload("message").build();

		this.sqsMessageHandlerWithQueueAutoCreate.setQueue("foo");
		this.sqsSendChannelWithAutoCreate.send(message);
		ArgumentCaptor<CreateQueueRequest> createQueueRequestArgumentCaptor =
				ArgumentCaptor.forClass(CreateQueueRequest.class);
		verify(this.amazonSqs).createQueue(createQueueRequestArgumentCaptor.capture());
		assertThat(createQueueRequestArgumentCaptor.getValue().queueName()).isEqualTo("foo");

		ArgumentCaptor<SendMessageRequest> sendMessageRequestArgumentCaptor =
				ArgumentCaptor.forClass(SendMessageRequest.class);
		verify(this.amazonSqs).sendMessage(sendMessageRequestArgumentCaptor.capture());
		assertThat(sendMessageRequestArgumentCaptor.getValue().queueUrl()).isEqualTo("https://queue-url.com/foo");
	}

	@Configuration
	@EnableIntegration
	public static class ContextConfiguration {

		@Bean
		@SuppressWarnings("unchecked")
		public SqsAsyncClient amazonSqs() {
			SqsAsyncClient amazonSqs = mock(SqsAsyncClient.class);

			willAnswer(invocation -> {
				GetQueueUrlRequest getQueueUrlRequest = (GetQueueUrlRequest) invocation.getArguments()[0];
				return CompletableFuture.completedFuture(
						GetQueueUrlResponse.builder()
								.queueUrl("https://queue-url.com/" + getQueueUrlRequest.queueName())
								.build());
			}).given(amazonSqs).getQueueUrl(any(GetQueueUrlRequest.class));

			willAnswer(invocation -> {
				CreateQueueRequest createQueueRequest = (CreateQueueRequest) invocation.getArguments()[0];
				return CompletableFuture.completedFuture(
						CreateQueueResponse.builder()
								.queueUrl("https://queue-url.com/" + createQueueRequest.queueName())
								.build());
			}).given(amazonSqs).createQueue(any(Consumer.class));

			given(amazonSqs.sendMessage(any(SendMessageRequest.class)))
					.willReturn(CompletableFuture.completedFuture(
							SendMessageResponse.builder()
									.messageId("testId")
									.sequenceNumber("1")
									.build()));

			return amazonSqs;
		}

		@Bean
		@ServiceActivator(inputChannel = "sqsSendChannel")
		public MessageHandler sqsMessageHandler() {
			return new SqsMessageHandler(amazonSqs());
		}

		@Bean
		@ServiceActivator(inputChannel = "sqsSendChannelWithAutoCreate")
		public MessageHandler sqsMessageHandlerWithQueueAutoCreate() {
			SqsMessageHandler sqsMessageHandler = new SqsMessageHandler(amazonSqs());
			sqsMessageHandler.setQueueNotFoundStrategy(QueueNotFoundStrategy.CREATE);
			return sqsMessageHandler;
		}

	}

}
