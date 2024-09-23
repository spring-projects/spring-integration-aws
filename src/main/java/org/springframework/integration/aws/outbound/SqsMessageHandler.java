/*
 * Copyright 2016-2024 the original author or authors.
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.awspring.cloud.sqs.QueueAttributesResolver;
import io.awspring.cloud.sqs.listener.QueueNotFoundStrategy;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.aws.support.SqsHeaderMapper;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.mapping.HeaderMapper;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * The {@link AbstractMessageHandler} implementation for the Amazon SQS
 * {@code sendMessage}.
 *
 * @author Artem Bilan
 * @author Rahul Pilani
 * @author Taylor Wicksell
 * @author Seth Kelly
 *
 * @see SqsAsyncClient#sendMessage(SendMessageRequest)
 * @see com.amazonaws.handlers.AsyncHandler
 *
 */
public class SqsMessageHandler extends AbstractAwsMessageHandler<Map<String, MessageAttributeValue>> {

	private final SqsAsyncClient amazonSqs;

	private MessageConverter messageConverter;

	private Expression queueExpression;

	private QueueNotFoundStrategy queueNotFoundStrategy = QueueNotFoundStrategy.FAIL;

	private Expression delayExpression;

	private Expression messageGroupIdExpression;

	private Expression messageDeduplicationIdExpression;

	public SqsMessageHandler(SqsAsyncClient amazonSqs) {
		Assert.notNull(amazonSqs, "'amazonSqs' must not be null");
		this.amazonSqs = amazonSqs;
	}

	public void setQueue(String queue) {
		Assert.hasText(queue, "'queue' must not be empty");
		setQueueExpression(new LiteralExpression(queue));
	}

	public void setQueueExpressionString(String queueExpression) {
		setQueueExpression(EXPRESSION_PARSER.parseExpression(queueExpression));
	}

	public void setQueueExpression(Expression queueExpression) {
		Assert.notNull(queueExpression, "'queueExpression' must not be null");
		this.queueExpression = queueExpression;
	}

	/**
	 * Set a {@link QueueNotFoundStrategy}; defaults to {@link QueueNotFoundStrategy#FAIL}.
	 * @param queueNotFoundStrategy the {@link QueueNotFoundStrategy} to use.
	 * @since 3.0
	 */
	public void setQueueNotFoundStrategy(QueueNotFoundStrategy queueNotFoundStrategy) {
		Assert.notNull(queueNotFoundStrategy, "'queueNotFoundStrategy' must not be null");
		this.queueNotFoundStrategy = queueNotFoundStrategy;
	}

	public void setDelay(int delaySeconds) {
		setDelayExpression(new ValueExpression<>(delaySeconds));
	}

	public void setDelayExpressionString(String delayExpression) {
		setDelayExpression(EXPRESSION_PARSER.parseExpression(delayExpression));
	}

	public void setDelayExpression(Expression delayExpression) {
		Assert.notNull(delayExpression, "'delayExpression' must not be null");
		this.delayExpression = delayExpression;
	}

	public void setMessageGroupId(String messageGroupId) {
		setMessageGroupIdExpression(new LiteralExpression(messageGroupId));
	}

	public void setMessageGroupIdExpressionString(String groupIdExpression) {
		setMessageGroupIdExpression(EXPRESSION_PARSER.parseExpression(groupIdExpression));
	}

	public void setMessageGroupIdExpression(Expression messageGroupIdExpression) {
		Assert.notNull(messageGroupIdExpression, "'messageGroupIdExpression' must not be null");
		this.messageGroupIdExpression = messageGroupIdExpression;
	}

	public void setMessageDeduplicationId(String messageDeduplicationId) {
		setMessageDeduplicationIdExpression(new LiteralExpression(messageDeduplicationId));
	}

	public void setMessageDeduplicationIdExpressionString(String messageDeduplicationIdExpression) {
		setMessageDeduplicationIdExpression(EXPRESSION_PARSER.parseExpression(messageDeduplicationIdExpression));
	}

	public void setMessageDeduplicationIdExpression(Expression messageDeduplicationIdExpression) {
		Assert.notNull(messageDeduplicationIdExpression, "'messageDeduplicationIdExpression' must not be null");
		this.messageDeduplicationIdExpression = messageDeduplicationIdExpression;
	}

	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	@Override
	protected void onInit() {
		super.onInit();
		setHeaderMapper(new SqsHeaderMapper());
		if (this.messageConverter == null) {
			this.messageConverter = new GenericMessageConverter(getConversionService());
		}
	}

	@Override
	protected AwsRequest messageToAwsRequest(Message<?> message) {
		Object payload = message.getPayload();
		if (payload instanceof SendMessageBatchRequest) {
			return (SendMessageBatchRequest) payload;
		}
		if (payload instanceof SendMessageRequest) {
			return (SendMessageRequest) payload;
		}

		SendMessageRequest.Builder sendMessageRequest = SendMessageRequest.builder();
		String queue = message.getHeaders().get(AwsHeaders.QUEUE, String.class);
		if (!StringUtils.hasText(queue) && this.queueExpression != null) {
			queue = this.queueExpression.getValue(getEvaluationContext(), message, String.class);
		}
		Assert.state(queue != null,
				"'queue' must not be null for sending an SQS message. "
						+ "Consider configuring this handler with a 'queue'( or 'queueExpression') or supply an "
						+ "'aws_queue' message header");

		String queueUrl = resolveQueueUrl(queue);

		String messageBody = (String) this.messageConverter.fromMessage(message, String.class);
		sendMessageRequest.queueUrl(queueUrl).messageBody(messageBody);

		if (this.delayExpression != null) {
			Integer delay = this.delayExpression.getValue(getEvaluationContext(), message, Integer.class);
			sendMessageRequest.delaySeconds(delay);
		}

		if (this.messageGroupIdExpression != null) {
			String messageGroupId =
					this.messageGroupIdExpression.getValue(getEvaluationContext(), message, String.class);
			sendMessageRequest.messageGroupId(messageGroupId);
		}

		if (this.messageDeduplicationIdExpression != null) {
			String messageDeduplicationId =
					this.messageDeduplicationIdExpression.getValue(getEvaluationContext(), message, String.class);
			sendMessageRequest.messageDeduplicationId(messageDeduplicationId);
		}

		mapHeaders(message, sendMessageRequest);
		return sendMessageRequest.build();
	}

	private String resolveQueueUrl(String queue) {
		return QueueAttributesResolver.builder()
				.sqsAsyncClient(this.amazonSqs)
				.queueNotFoundStrategy(this.queueNotFoundStrategy)
				.queueAttributeNames(Collections.emptyList())
				.queueName(queue)
				.build()
				.resolveQueueAttributes()
				.join()
				.getQueueUrl();
	}

	private void mapHeaders(Message<?> message, SendMessageRequest.Builder sendMessageRequest) {
		HeaderMapper<Map<String, MessageAttributeValue>> headerMapper = getHeaderMapper();
		if (headerMapper != null) {
			HashMap<String, MessageAttributeValue> messageAttributes = new HashMap<>();
			headerMapper.fromHeaders(message.getHeaders(), messageAttributes);
			if (!messageAttributes.isEmpty()) {
				sendMessageRequest.messageAttributes(messageAttributes);
			}
		}
	}

	@Override
	protected CompletableFuture<? extends AwsResponse> handleMessageToAws(Message<?> message, AwsRequest request) {
		if (request instanceof SendMessageBatchRequest sendMessageBatchRequest) {
			return this.amazonSqs.sendMessageBatch(sendMessageBatchRequest);
		}
		else {
			return this.amazonSqs.sendMessage((SendMessageRequest) request);
		}
	}

	@Override
	protected Map<String, ?> additionalOnSuccessHeaders(AwsRequest request, AwsResponse response) {
		if (response instanceof SendMessageResponse sendMessageResponse) {
			Map<String, Object> headers = new HashMap<>();
			headers.put(AwsHeaders.MESSAGE_ID, sendMessageResponse.messageId());
			String sequenceNumber = sendMessageResponse.sequenceNumber();
			if (StringUtils.hasText(sequenceNumber)) {
				headers.put(AwsHeaders.SEQUENCE_NUMBER, sequenceNumber);
			}
			return headers;
		}
		return null;
	}

}
