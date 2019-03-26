/*
 * Copyright 2015-2016 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.support.MessageBuilder;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * Parent class to contain tests that exercise the SqsMessageHandler class
 *
 * Subclasses can instantiate SqsMessageHandler in their own way.
 *
 * @author Rahul Pilani
 * @author Artem Bilan
 */
public abstract class AbstractSqsMessageHandlerTests {

	@Autowired
	protected AmazonSQS amazonSqs;

	@Autowired
	protected MessageChannel sqsSendChannel;

	@Autowired
	protected SqsMessageHandler sqsMessageHandler;

	@Test
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
		verify(this.amazonSqs).sendMessage(sendMessageRequestArgumentCaptor.capture());
		assertThat(sendMessageRequestArgumentCaptor.getValue().getQueueUrl())
				.isEqualTo("https://queue-url.com/foo");

		message = MessageBuilder.withPayload("message").setHeader(AwsHeaders.QUEUE, "bar").build();
		this.sqsSendChannel.send(message);
		verify(this.amazonSqs, times(2)).sendMessage(sendMessageRequestArgumentCaptor.capture());
		assertThat(sendMessageRequestArgumentCaptor.getValue().getQueueUrl())
				.isEqualTo("https://queue-url.com/bar");

		SpelExpressionParser spelExpressionParser = new SpelExpressionParser();
		Expression expression = spelExpressionParser.parseExpression("headers.foo");
		this.sqsMessageHandler.setQueueExpression(expression);
		message = MessageBuilder.withPayload("message").setHeader("foo", "baz").build();
		this.sqsSendChannel.send(message);
		verify(this.amazonSqs, times(3)).sendMessage(sendMessageRequestArgumentCaptor.capture());
		assertThat(sendMessageRequestArgumentCaptor.getValue().getQueueUrl())
				.isEqualTo("https://queue-url.com/baz");
	}

}
