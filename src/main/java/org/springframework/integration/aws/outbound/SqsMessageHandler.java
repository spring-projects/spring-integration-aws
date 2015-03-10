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

import org.springframework.cloud.aws.core.env.ResourceIdResolver;
import org.springframework.cloud.aws.messaging.core.QueueMessagingTemplate;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.expression.IntegrationEvaluationContextAware;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.amazonaws.services.sqs.AmazonSQS;

/**
 * The {@link AbstractMessageHandler} implementation for the Amazon SQS {@code sendMessage}.
 *
 * @author Artem Bilan
 * @author Rahul Pilani
 *
 * @see QueueMessagingTemplate
 * @see org.springframework.cloud.aws.messaging.core.QueueMessageChannel
 */
public class SqsMessageHandler extends AbstractMessageHandler implements IntegrationEvaluationContextAware {

	private final QueueMessagingTemplate template;

	private Expression queueExpression;

	private EvaluationContext evaluationContext;

	public SqsMessageHandler(AmazonSQS amazonSqs) {
		this(amazonSqs, null);
	}

	public SqsMessageHandler(AmazonSQS amazonSqs, ResourceIdResolver resourceIdResolver) {
		this(new QueueMessagingTemplate(amazonSqs, resourceIdResolver));
	}

	public SqsMessageHandler(QueueMessagingTemplate template) {
		Assert.notNull(template, "template must not be null.");
		this.template = template;
	}

	public void setQueue(String queue) {
		Assert.hasText(queue, "'queue' must not be empty");
		this.queueExpression = new LiteralExpression(queue);
	}

	public void setQueueExpression(Expression queueExpression) {
		Assert.notNull(queueExpression, "'queueExpression' must not be null");
		this.queueExpression = queueExpression;
	}

	@Override
	public void setIntegrationEvaluationContext(EvaluationContext evaluationContext) {
		this.evaluationContext = evaluationContext;
	}

	@Override
	protected void onInit() throws Exception {
		Assert.notNull(this.evaluationContext);
	}

	@Override
	protected void handleMessageInternal(Message<?> message) throws Exception {
		String queue = message.getHeaders().get(AwsHeaders.QUEUE, String.class);
		if (!StringUtils.hasText(queue) && this.queueExpression != null) {
			queue = this.queueExpression.getValue(this.evaluationContext, message, String.class);
		}
		Assert.state(queue != null, "'queue' must not be null for sending an SQS message. " +
				"Consider configuring this handler with a 'queue'( or 'queueExpression') or supply an " +
				"'aws_queue' message header");
		this.template.send(queue, message);
	}

}
