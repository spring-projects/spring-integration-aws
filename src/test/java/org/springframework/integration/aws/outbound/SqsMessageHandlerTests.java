/*
 * Copyright 2015-2018 the original author or authors.
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

package org.springframework.integration.aws.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.aws.messaging.support.destination.DynamicQueueUrlDestinationResolver;
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
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * Instantiating SqsMessageHandler using amazonSqs.
 *
 * @author Artem Bilan
 * @author Rahul Pilani
 * @author Seth Kelly
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class SqsMessageHandlerTests {

	@Autowired
	protected AmazonSQSAsync amazonSqs;

	@Autowired
	protected MessageChannel sqsSendChannel;

	@Autowired
	protected MessageChannel sqsSendChannelWithAutoCreate;

	@Autowired
	protected SqsMessageHandler sqsMessageHandler;

	@Autowired
	protected SqsMessageHandler sqsMessageHandlerWithAutoQueueCreate;

	@Test
	@SuppressWarnings("unchecked")
	public void testSqsMessageHandler() {
		Message<String> message = MessageBuilder.withPayload("message").build();
		try {
			this.sqsSendChannel.send(message);
		}
		catch (Exception e) {
			assertThat(e).isInstanceOf(MessageHandlingException.class);
			assertThat(e.getCause()).isInstanceOf(IllegalStateException.class);
		}

		this.sqsMessageHandler.setQueue("foo");
		this.sqsSendChannel.send(message);
		ArgumentCaptor<SendMessageRequest> sendMessageRequestArgumentCaptor =
				ArgumentCaptor.forClass(SendMessageRequest.class);
		verify(this.amazonSqs)
				.sendMessageAsync(sendMessageRequestArgumentCaptor.capture(), any(AsyncHandler.class));
		assertThat(sendMessageRequestArgumentCaptor.getValue().getQueueUrl())
				.isEqualTo("http://queue-url.com/foo");

		message = MessageBuilder.withPayload("message").setHeader(AwsHeaders.QUEUE, "bar").build();
		this.sqsSendChannel.send(message);
		verify(this.amazonSqs, times(2))
				.sendMessageAsync(sendMessageRequestArgumentCaptor.capture(), any(AsyncHandler.class));

		assertThat(sendMessageRequestArgumentCaptor.getValue().getQueueUrl())
				.isEqualTo("http://queue-url.com/bar");

		SpelExpressionParser spelExpressionParser = new SpelExpressionParser();
		Expression expression = spelExpressionParser.parseExpression("headers.foo");
		this.sqsMessageHandler.setQueueExpression(expression);
		message = MessageBuilder.withPayload("message").setHeader("foo", "baz").build();
		this.sqsSendChannel.send(message);
		verify(this.amazonSqs, times(3))
				.sendMessageAsync(sendMessageRequestArgumentCaptor.capture(), any(AsyncHandler.class));

		SendMessageRequest sendMessageRequestArgumentCaptorValue = sendMessageRequestArgumentCaptor.getValue();
		assertThat(sendMessageRequestArgumentCaptorValue.getQueueUrl())
				.isEqualTo("http://queue-url.com/baz");

		Map<String, MessageAttributeValue> messageAttributes =
				sendMessageRequestArgumentCaptorValue.getMessageAttributes();

		assertThat(messageAttributes).doesNotContainKey(MessageHeaders.ID);
		assertThat(messageAttributes).doesNotContainKey(MessageHeaders.TIMESTAMP);
		assertThat(messageAttributes).containsKey("foo");
		assertThat(messageAttributes.get("foo").getStringValue()).isEqualTo("baz");

	}

	@Test
	@SuppressWarnings("unchecked")
	public void testSqsMessageHandlerWithAutoQueueCreate() {
		Message<String> message = MessageBuilder.withPayload("message").build();

		this.sqsMessageHandlerWithAutoQueueCreate.setQueue("foo");
		this.sqsSendChannelWithAutoCreate.send(message);
		ArgumentCaptor<CreateQueueRequest> createQueueRequestArgumentCaptor =
				ArgumentCaptor.forClass(CreateQueueRequest.class);
		verify(this.amazonSqs).createQueue(createQueueRequestArgumentCaptor.capture());
		assertThat(createQueueRequestArgumentCaptor.getValue().getQueueName().equals("foo"));

		ArgumentCaptor<SendMessageRequest> sendMessageRequestArgumentCaptor =
				ArgumentCaptor.forClass(SendMessageRequest.class);
		verify(this.amazonSqs)
				.sendMessageAsync(sendMessageRequestArgumentCaptor.capture(), any(AsyncHandler.class));
		assertThat(sendMessageRequestArgumentCaptor.getValue().getQueueUrl())
				.isEqualTo("http://queue-url.com/foo");
	}

	@Configuration
	@EnableIntegration
	public static class ContextConfiguration {

		@Bean
		public AmazonSQSAsync amazonSqs() {
			AmazonSQSAsync amazonSqs = mock(AmazonSQSAsync.class);

			willAnswer(invocation -> {
				GetQueueUrlRequest getQueueUrlRequest = (GetQueueUrlRequest) invocation.getArguments()[0];
				GetQueueUrlResult queueUrl = new GetQueueUrlResult();
				queueUrl.setQueueUrl("http://queue-url.com/" + getQueueUrlRequest.getQueueName());
				return queueUrl;
			})
					.given(amazonSqs)
					.getQueueUrl(any(GetQueueUrlRequest.class));

			willAnswer(invocation -> {
				CreateQueueRequest createQueueRequest = (CreateQueueRequest) invocation.getArguments()[0];
				CreateQueueResult queueUrl = new CreateQueueResult();
				queueUrl.setQueueUrl("http://queue-url.com/" + createQueueRequest.getQueueName());
				return queueUrl;
			})
					.given(amazonSqs)
					.createQueue(any(CreateQueueRequest.class));

			return amazonSqs;
		}

		@Bean
		@ServiceActivator(inputChannel = "sqsSendChannel")
		public MessageHandler sqsMessageHandler() {
			return new SqsMessageHandler(amazonSqs());
		}

		@Bean
		@ServiceActivator(inputChannel = "sqsSendChannelWithAutoCreate")
		public MessageHandler sqsMessageHandlerWithAutoQueueCreate() {
			DynamicQueueUrlDestinationResolver destinationResolver = new DynamicQueueUrlDestinationResolver(amazonSqs(), null);
			destinationResolver.setAutoCreate(true);
			return new SqsMessageHandler(amazonSqs(), destinationResolver);
		}

	}

}
