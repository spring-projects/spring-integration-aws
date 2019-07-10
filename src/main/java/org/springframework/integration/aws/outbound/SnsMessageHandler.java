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
import org.springframework.expression.Expression;
import org.springframework.expression.TypeLocator;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.support.StandardTypeLocator;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.aws.support.SnsBodyBuilder;
import org.springframework.integration.aws.support.SnsHeaderMapper;
import org.springframework.integration.mapping.HeaderMapper;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

/**
 * The {@link AbstractAwsMessageHandler} implementation to send SNS Notifications
 * ({@link AmazonSNSAsync#publishAsync(PublishRequest)}) to the provided {@code topicArn}
 * (or evaluated at runtime against {@link Message}).
 * <p>
 * The SNS Message subject can be evaluated as a result of {@link #subjectExpression}.
 * <p>
 * The algorithm to populate SNS Message body is like:
 * <ul>
 * <li>If the {@code payload instanceof PublishRequest} it is used as is for publishing.
 * </li>
 * <li>If the {@link #bodyExpression} is specified, it is used to be evaluated against
 * {@code requestMessage}.</li>
 * <li>If the evaluation result (or {@code payload}) is instance of
 * {@link SnsBodyBuilder}, the SNS Message is built from there and the
 * {@code messageStructure} of the {@link PublishRequest} is set to {@code json}. For the
 * convenience the package {@code org.springframework.integration.aws.support} is imported
 * to the {@link #getEvaluationContext()} to allow bypass it for the
 * {@link SnsBodyBuilder} from the {@link #bodyExpression} definition. For example:
 * <pre class="code">
 * {@code
 * String bodyExpression =
 * "SnsBodyBuilder.withDefault(payload).forProtocols(payload.substring(0, 140), 'sms')";
 * snsMessageHandler.setBodyExpression(spelExpressionParser.parseExpression(bodyExpression));
 * }
 * </pre></li>
 * <li>Otherwise the {@code payload} (or the {@link #bodyExpression} evaluation result) is
 * converted to the {@link String} using {@link #getConversionService()}.</li>
 * </ul>
 *
 * @author Artem Bilan
 * @see AmazonSNSAsync
 * @see PublishRequest
 * @see SnsBodyBuilder
 */
public class SnsMessageHandler extends AbstractAwsMessageHandler<Map<String, MessageAttributeValue>> {

	private final AmazonSNSAsync amazonSns;

	private Expression topicArnExpression;

	private Expression subjectExpression;

	private Expression bodyExpression;

	private ResourceIdResolver resourceIdResolver;

	public SnsMessageHandler(AmazonSNSAsync amazonSns) {
		Assert.notNull(amazonSns, "amazonSns must not be null.");
		this.amazonSns = amazonSns;
		doSetHeaderMapper(new SnsHeaderMapper());
	}

	public void setTopicArn(String topicArn) {
		Assert.hasText(topicArn, "topicArn must not be empty.");
		this.topicArnExpression = new LiteralExpression(topicArn);
	}

	public void setTopicArnExpression(Expression topicArnExpression) {
		Assert.notNull(topicArnExpression, "topicArnExpression must not be null.");
		this.topicArnExpression = topicArnExpression;
	}

	public void setSubject(String subject) {
		Assert.hasText(subject, "subject must not be empty.");
		this.subjectExpression = new LiteralExpression(subject);
	}

	public void setSubjectExpression(Expression subjectExpression) {
		Assert.notNull(subjectExpression, "subjectExpression must not be null.");
		this.subjectExpression = subjectExpression;
	}

	/**
	 * The {@link Expression} to produce the SNS notification message. If it evaluates to
	 * the {@link SnsBodyBuilder} the {@code messageStructure} of the
	 * {@link PublishRequest} is set to {@code json}. Otherwise the
	 * {@link #getConversionService()} is used to convert the evaluation result to the
	 * {@link String} without setting the {@code messageStructure}.
	 * @param bodyExpression the {@link Expression} to produce the SNS notification
	 * message.
	 */
	public void setBodyExpression(Expression bodyExpression) {
		Assert.notNull(bodyExpression, "bodyExpression must not be null.");
		this.bodyExpression = bodyExpression;
	}

	/**
	 * Specify a {@link ResourceIdResolver} to resolve logical topic names to physical
	 * resource ids.
	 * @param resourceIdResolver the {@link ResourceIdResolver} to use.
	 */
	public void setResourceIdResolver(ResourceIdResolver resourceIdResolver) {
		this.resourceIdResolver = resourceIdResolver;
	}

	@Override
	protected void onInit() {
		super.onInit();
		TypeLocator typeLocator = getEvaluationContext().getTypeLocator();
		if (typeLocator instanceof StandardTypeLocator) {
			/*
			 * Register the 'org.springframework.integration.aws.support' package you
			 * don't need a FQCN for the 'SnsMessageBuilder'.
			 */
			((StandardTypeLocator) typeLocator).registerImport("org.springframework.integration.aws.support");
		}
	}

	@Override
	protected Future<?> handleMessageToAws(Message<?> message) {
		Object payload = message.getPayload();

		PublishRequest publishRequest = null;

		if (payload instanceof PublishRequest) {
			publishRequest = (PublishRequest) payload;
		}
		else {
			Assert.state(this.topicArnExpression != null, "'topicArn' or 'topicArnExpression' must be specified.");
			publishRequest = new PublishRequest();
			String topicArn = this.topicArnExpression.getValue(getEvaluationContext(), message, String.class);
			if (this.resourceIdResolver != null) {
				topicArn = this.resourceIdResolver.resolveToPhysicalResourceId(topicArn);
			}
			publishRequest.setTopicArn(topicArn);

			if (this.subjectExpression != null) {
				String subject = this.subjectExpression.getValue(getEvaluationContext(), message, String.class);
				publishRequest.setSubject(subject);
			}

			Object snsMessage = message.getPayload();

			if (this.bodyExpression != null) {
				snsMessage = this.bodyExpression.getValue(getEvaluationContext(), message);
			}

			if (snsMessage instanceof SnsBodyBuilder) {
				publishRequest.withMessageStructure("json").setMessage(((SnsBodyBuilder) snsMessage).build());
			}
			else {
				publishRequest.setMessage(getConversionService().convert(snsMessage, String.class));
			}

			HeaderMapper<Map<String, MessageAttributeValue>> headerMapper = getHeaderMapper();
			if (headerMapper != null) {
				mapHeaders(message, publishRequest, headerMapper);
			}
		}

		AsyncHandler<PublishRequest, PublishResult> asyncHandler = obtainAsyncHandler(message, publishRequest);
		return this.amazonSns.publishAsync(publishRequest, asyncHandler);

	}

	private void mapHeaders(Message<?> message, PublishRequest publishRequest,
			HeaderMapper<Map<String, MessageAttributeValue>> headerMapper) {

		HashMap<String, MessageAttributeValue> messageAttributes = new HashMap<>();
		headerMapper.fromHeaders(message.getHeaders(), messageAttributes);
		if (!messageAttributes.isEmpty()) {
			publishRequest.setMessageAttributes(messageAttributes);
		}
	}

	@Override
	protected void additionalOnSuccessHeaders(AbstractIntegrationMessageBuilder<?> messageBuilder,
			AmazonWebServiceRequest request, Object result) {

		if (request instanceof PublishRequest) {
			PublishRequest publishRequest = (PublishRequest) request;

			messageBuilder.setHeader(AwsHeaders.TOPIC, publishRequest.getTopicArn());
		}

		if (result instanceof PublishResult) {
			PublishResult publishResult = (PublishResult) result;

			messageBuilder.setHeader(AwsHeaders.MESSAGE_ID, publishResult.getMessageId());
		}
	}

}
