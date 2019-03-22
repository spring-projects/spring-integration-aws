/*
 * Copyright 2016-2019 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import org.springframework.cloud.aws.core.env.ResourceIdResolver;
import org.springframework.cloud.aws.messaging.support.destination.DynamicQueueUrlDestinationResolver;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.aws.support.SqsHeaderMapper;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.mapping.HeaderMapper;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.core.DestinationResolver;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

/**
 * The {@link AbstractMessageHandler} implementation for the Amazon SQS {@code sendMessage}.
 *
 * @author Artem Bilan
 * @author Rahul Pilani
 * @author Taylor Wicksell
 * @author Seth Kelly
 *
 * @see AmazonSQSAsync#sendMessageAsync(SendMessageRequest, AsyncHandler)
 * @see com.amazonaws.handlers.AsyncHandler

 */
public class SqsMessageHandler extends AbstractAwsMessageHandler<Map<String, MessageAttributeValue>> {

	private final AmazonSQSAsync amazonSqs;

	private final DestinationResolver<?> destinationResolver;

	private MessageConverter messageConverter;

	private Expression queueExpression;

	private Expression delayExpression;

	private Expression messageGroupIdExpression;

	private Expression messageDeduplicationIdExpression;


	public SqsMessageHandler(AmazonSQSAsync amazonSqs) {
		this(amazonSqs, (ResourceIdResolver) null);
	}

	public SqsMessageHandler(AmazonSQSAsync amazonSqs, ResourceIdResolver resourceIdResolver) {
		this(amazonSqs, new DynamicQueueUrlDestinationResolver(amazonSqs, resourceIdResolver));
	}

	public SqsMessageHandler(AmazonSQSAsync amazonSqs, DestinationResolver<?> destinationResolver) {
		Assert.notNull(amazonSqs, "'amazonSqs' must not be null");
		Assert.notNull(destinationResolver, "'destinationResolver' must not be null");

		this.amazonSqs = amazonSqs;
		this.destinationResolver = destinationResolver;
		doSetHeaderMapper(new SqsHeaderMapper());
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

		if (this.messageConverter == null) {
			this.messageConverter = new GenericMessageConverter(getConversionService());
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Future<?> handleMessageToAws(Message<?> message) {
		Object payload = message.getPayload();
		if (payload instanceof SendMessageBatchRequest) {
			AsyncHandler<SendMessageBatchRequest, SendMessageBatchResult> asyncHandler =
					obtainAsyncHandler(message, (SendMessageBatchRequest) payload);
			return this.amazonSqs.sendMessageBatchAsync((SendMessageBatchRequest) payload, asyncHandler);
		}

		SendMessageRequest sendMessageRequest;
		if (payload instanceof SendMessageRequest) {
			sendMessageRequest = (SendMessageRequest) payload;
		}
		else {
			String queue = message.getHeaders().get(AwsHeaders.QUEUE, String.class);
			if (!StringUtils.hasText(queue) && this.queueExpression != null) {
				queue = this.queueExpression.getValue(getEvaluationContext(), message, String.class);
			}
			Assert.state(queue != null, "'queue' must not be null for sending an SQS message. " +
					"Consider configuring this handler with a 'queue'( or 'queueExpression') or supply an " +
					"'aws_queue' message header");

			String queueUrl = (String) this.destinationResolver.resolveDestination(queue);
			String messageBody = (String) this.messageConverter.fromMessage(message, String.class);
			sendMessageRequest = new SendMessageRequest(queueUrl, messageBody);

			if (this.delayExpression != null) {
				Integer delay = this.delayExpression.getValue(getEvaluationContext(), message, Integer.class);
				sendMessageRequest.setDelaySeconds(delay);
			}

			if (this.messageGroupIdExpression != null) {
				String messageGroupId =
						this.messageGroupIdExpression.getValue(getEvaluationContext(), message, String.class);
				sendMessageRequest.setMessageGroupId(messageGroupId);
			}

			if (this.messageDeduplicationIdExpression != null) {
				String messageDeduplicationId =
						this.messageDeduplicationIdExpression.getValue(getEvaluationContext(), message, String.class);
				sendMessageRequest.setMessageDeduplicationId(messageDeduplicationId);
			}

			HeaderMapper<Map<String, MessageAttributeValue>> headerMapper = getHeaderMapper();
			if (headerMapper != null) {
				mapHeaders(message, sendMessageRequest, headerMapper);
			}
		}
		AsyncHandler<SendMessageRequest, SendMessageResult> asyncHandler =
				obtainAsyncHandler(message, sendMessageRequest);
		return this.amazonSqs.sendMessageAsync(sendMessageRequest, asyncHandler);
	}

	private void mapHeaders(Message<?> message, SendMessageRequest sendMessageRequest,
			HeaderMapper<Map<String, MessageAttributeValue>> headerMapper) {

		HashMap<String, MessageAttributeValue> messageAttributes = new HashMap<>();
		headerMapper.fromHeaders(message.getHeaders(), messageAttributes);
		if (!messageAttributes.isEmpty()) {
			sendMessageRequest.setMessageAttributes(messageAttributes);
		}
	}

	@Override
	protected void additionalOnSuccessHeaders(AbstractIntegrationMessageBuilder<?> messageBuilder,
			AmazonWebServiceRequest request, Object result) {

		if (result instanceof SendMessageResult) {
			SendMessageResult sendMessageResult = (SendMessageResult) result;
			messageBuilder.setHeaderIfAbsent(AwsHeaders.MESSAGE_ID, sendMessageResult.getMessageId());
			messageBuilder.setHeaderIfAbsent(AwsHeaders.SEQUENCE_NUMBER, sendMessageResult.getSequenceNumber());
		}
	}

}
