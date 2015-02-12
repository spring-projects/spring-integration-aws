/*
 * Copyright 2015 the original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.springframework.integration.aws.outbound;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * @author Artem Bilan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class SqsMessageHandlerTests {

	@Autowired
	private AmazonSQS amazonSqs;

	@Autowired
	private MessageChannel sqsSendChannel;

	@Autowired
	private SqsMessageHandler sqsMessageHandler;

	@Test
	public void testSqsMessageHandler() {
		Message<String> message = MessageBuilder.withPayload("message").build();
		try {
			this.sqsSendChannel.send(message);
		}
		catch (Exception e) {
			assertThat(e, instanceOf(MessageHandlingException.class));
			assertThat(e.getCause(), instanceOf(IllegalStateException.class));
		}

		this.sqsMessageHandler.setQueue("foo");
		this.sqsSendChannel.send(message);
		ArgumentCaptor<SendMessageRequest> sendMessageRequestArgumentCaptor =
				ArgumentCaptor.forClass(SendMessageRequest.class);
		verify(this.amazonSqs).sendMessage(sendMessageRequestArgumentCaptor.capture());
		assertEquals("http://queue-url.com/foo", sendMessageRequestArgumentCaptor.getValue().getQueueUrl());

		message = MessageBuilder.withPayload("message").setHeader(AwsHeaders.QUEUE, "bar").build();
		this.sqsSendChannel.send(message);
		verify(this.amazonSqs, times(2)).sendMessage(sendMessageRequestArgumentCaptor.capture());
		assertEquals("http://queue-url.com/bar", sendMessageRequestArgumentCaptor.getValue().getQueueUrl());

		SpelExpressionParser spelExpressionParser = new SpelExpressionParser();
		Expression expression = spelExpressionParser.parseExpression("headers.foo");
		this.sqsMessageHandler.setQueueExpression(expression);
		message = MessageBuilder.withPayload("message").setHeader("foo", "baz").build();
		this.sqsSendChannel.send(message);
		verify(this.amazonSqs, times(3)).sendMessage(sendMessageRequestArgumentCaptor.capture());
		assertEquals("http://queue-url.com/baz", sendMessageRequestArgumentCaptor.getValue().getQueueUrl());
	}

	@Configuration
	@EnableIntegration
	public static class ContextConfiguration {

		@Bean
		public AmazonSQS amazonSqs() {
			AmazonSQS amazonSqs = mock(AmazonSQS.class);

			doAnswer(new Answer<GetQueueUrlResult>() {

				@Override
				public GetQueueUrlResult answer(InvocationOnMock invocation) throws Throwable {
					GetQueueUrlRequest getQueueUrlRequest = (GetQueueUrlRequest) invocation.getArguments()[0];
					GetQueueUrlResult queueUrl = new GetQueueUrlResult();
					queueUrl.setQueueUrl("http://queue-url.com/" + getQueueUrlRequest.getQueueName());
					return queueUrl;
				}

			}).when(amazonSqs).getQueueUrl(any(GetQueueUrlRequest.class));

			return amazonSqs;
		}

		@Bean
		@ServiceActivator(inputChannel = "sqsSendChannel")
		public MessageHandler sqsMessageHandler() {
			return new SqsMessageHandler(amazonSqs());
		}

	}

}
