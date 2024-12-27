/*
 * Copyright 2015-2024 the original author or authors.
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
import java.util.concurrent.atomic.AtomicReference;

import io.awspring.cloud.sqs.listener.QueueNotFoundStrategy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.aws.LocalstackContainerTest;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Instantiating SqsMessageHandler using amazonSqs.
 *
 * @author Artem Bilan
 * @author Rahul Pilani
 * @author Seth Kelly
 */
@SpringJUnitConfig
class SqsMessageHandlerTests implements LocalstackContainerTest {

	private static final AtomicReference<String> fooUrl = new AtomicReference<>();

	private static final AtomicReference<String> barUrl = new AtomicReference<>();

	private static final AtomicReference<String> bazUrl = new AtomicReference<>();

	private static SqsAsyncClient AMAZON_SQS;

	@Autowired
	protected MessageChannel sqsSendChannel;

	@Autowired
	protected MessageChannel sqsSendChannelWithAutoCreate;

	@Autowired
	protected SqsMessageHandler sqsMessageHandler;

	@BeforeAll
	static void setup() {
		AMAZON_SQS = LocalstackContainerTest.sqsClient();
		CompletableFuture<?> foo =
				AMAZON_SQS.createQueue(request -> request.queueName("foo"))
						.thenAccept(response -> fooUrl.set(response.queueUrl()));
		CompletableFuture<?> bar =
				AMAZON_SQS.createQueue(request -> request.queueName("bar"))
						.thenAccept(response -> barUrl.set(response.queueUrl()));
		CompletableFuture<?> baz =
				AMAZON_SQS.createQueue(request -> request.queueName("baz"))
						.thenAccept(response -> bazUrl.set(response.queueUrl()));

		CompletableFuture.allOf(foo, bar, baz).join();
	}

	@Test
	void sqsMessageHandler() {
		final Message<String> message = MessageBuilder.withPayload("message").build();

		assertThatExceptionOfType(MessageHandlingException.class)
				.isThrownBy(() -> this.sqsSendChannel.send(message))
				.withCauseInstanceOf(IllegalStateException.class);

		this.sqsMessageHandler.setQueue("foo");
		this.sqsSendChannel.send(message);

		ReceiveMessageResponse receiveMessageResponse =
				AMAZON_SQS.receiveMessage(request -> request.queueUrl(fooUrl.get()).waitTimeSeconds(10))
						.join();

		assertThat(receiveMessageResponse.hasMessages()).isTrue();
		assertThat(receiveMessageResponse.messages().get(0).body()).isEqualTo("message");

		Message<String> message2 = MessageBuilder.withPayload("message").setHeader(AwsHeaders.QUEUE, "bar").build();
		this.sqsSendChannel.send(message2);

		receiveMessageResponse =
				AMAZON_SQS.receiveMessage(request -> request.queueUrl(barUrl.get()).waitTimeSeconds(10))
						.join();

		assertThat(receiveMessageResponse.hasMessages()).isTrue();
		assertThat(receiveMessageResponse.messages().get(0).body()).isEqualTo("message");


		SpelExpressionParser spelExpressionParser = new SpelExpressionParser();
		Expression expression = spelExpressionParser.parseExpression("headers.foo");
		this.sqsMessageHandler.setQueueExpression(expression);
		message2 = MessageBuilder.withPayload("message").setHeader("foo", "baz").build();
		this.sqsSendChannel.send(message2);

		receiveMessageResponse =
				AMAZON_SQS.receiveMessage(request ->
								request.queueUrl(bazUrl.get())
										.messageAttributeNames(QueueAttributeName.ALL.toString())
										.waitTimeSeconds(10))
						.join();

		assertThat(receiveMessageResponse.hasMessages()).isTrue();
		software.amazon.awssdk.services.sqs.model.Message message1 = receiveMessageResponse.messages().get(0);
		assertThat(message1.body()).isEqualTo("message");

		Map<String, MessageAttributeValue> messageAttributes = message1.messageAttributes();

		assertThat(messageAttributes)
				.doesNotContainKeys(MessageHeaders.ID, MessageHeaders.TIMESTAMP)
				.containsKey("foo");
		assertThat(messageAttributes.get("foo").stringValue()).isEqualTo("baz");
	}

	@Test
	void sqsMessageHandlerWithAutoQueueCreate() {
		Message<String> message = MessageBuilder.withPayload("message").build();

		this.sqsSendChannelWithAutoCreate.send(message);

		ReceiveMessageResponse autoCreateQueueResponse =
				AMAZON_SQS.getQueueUrl(request -> request.queueName("autoCreateQueue"))
						.thenCompose(response ->
								AMAZON_SQS.receiveMessage(request ->
										request.queueUrl(response.queueUrl()).waitTimeSeconds(10)))
						.join();

		assertThat(autoCreateQueueResponse.hasMessages()).isTrue();
		assertThat(autoCreateQueueResponse.messages().get(0).body()).isEqualTo("message");
	}

	@Configuration
	@EnableIntegration
	public static class ContextConfiguration {

		@Bean
		@ServiceActivator(inputChannel = "sqsSendChannel")
		public MessageHandler sqsMessageHandler() {
			return new SqsMessageHandler(AMAZON_SQS);
		}

		@Bean
		@ServiceActivator(inputChannel = "sqsSendChannelWithAutoCreate")
		public MessageHandler sqsMessageHandlerWithQueueAutoCreate() {
			SqsMessageHandler sqsMessageHandler = new SqsMessageHandler(AMAZON_SQS);
			sqsMessageHandler.setQueueNotFoundStrategy(QueueNotFoundStrategy.CREATE);
			sqsMessageHandler.setQueue("autoCreateQueue");
			return sqsMessageHandler;
		}

	}

}
