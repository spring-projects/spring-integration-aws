/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.integration.aws.inbound;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.cloud.aws.messaging.endpoint.NotificationStatus;
import org.springframework.cloud.aws.messaging.endpoint.NotificationStatusHandlerMethodArgumentResolver;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.http.inbound.HttpRequestHandlingMessagingGateway;
import org.springframework.integration.http.inbound.RequestMapping;
import org.springframework.integration.mapping.HeaderMapper;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import org.springframework.web.multipart.MultipartResolver;

import com.amazonaws.services.sns.AmazonSNS;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * The {@link HttpRequestHandlingMessagingGateway} extension for the Amazon WS SNS HTTP(S) endpoints.
 * Accepts all {@code x-amz-sns-message-type}s, converts the received Topic JSON message to the
 * {@link Map} using {@link MappingJackson2HttpMessageConverter} and send it to the provided
 * {@link #requestChannel} as {@link Message} {@code payload}.
 * <p>
 * The mapped url must be configured inside the Amazon Web Service platform as a subscription.
 * Before receiving any notification itself this HTTP endpoint must confirm the subscription.
 * <p>
 * The {@link #handleNotificationStatus} flag (defaults to {@code false}) indicates that
 * this endpoint should send the {@code SubscriptionConfirmation/UnsubscribeConfirmation}
 * messages to the the provided {@link #requestChannel}. If that, the {@link AwsHeaders#NOTIFICATION_STATUS}
 * header is populated with the {@link NotificationStatus} value. In that case it is a responsibility of
 * the application to {@link NotificationStatus#confirmSubscription()} or not.
 * <p>
 * By default this endpoint just does {@link NotificationStatus#confirmSubscription()}
 * for the {@code SubscriptionConfirmation} message type.
 * And does nothing for the {@code UnsubscribeConfirmation}.
 * <p>
 * For the convenience on the underlying message flow routing a {@link AwsHeaders#SNS_MESSAGE_TYPE}
 * header is present.
 * @author Artem Bilan
 */
public class SnsInboundChannelAdapter extends HttpRequestHandlingMessagingGateway {

	private final NotificationStatusResolver notificationStatusResolver;

	private final MappingJackson2HttpMessageConverter jackson2HttpMessageConverter =
			new MappingJackson2HttpMessageConverter();

	private volatile boolean handleNotificationStatus;

	private volatile Expression payloadExpression;

	private EvaluationContext evaluationContext;

	public SnsInboundChannelAdapter(AmazonSNS amazonSns, String... path) {
		super(false);
		Assert.notNull(amazonSns, "'amazonSns' must not be null.");
		Assert.notNull(path, "'path' must not be null.");
		Assert.noNullElements(path, "'path' must not contain null elements.");
		this.notificationStatusResolver = new NotificationStatusResolver(amazonSns);
		RequestMapping requestMapping = new RequestMapping();
		requestMapping.setMethods(HttpMethod.POST);
		requestMapping.setHeaders("x-amz-sns-message-type");
		requestMapping.setPathPatterns(path);
		super.setRequestMapping(requestMapping);
		super.setStatusCodeExpression(new ValueExpression<>(HttpStatus.NO_CONTENT));
		super.setMessageConverters(
				Collections.<HttpMessageConverter<?>>singletonList(this.jackson2HttpMessageConverter));
		super.setRequestPayloadType(HashMap.class);
	}

	public void setHandleNotificationStatus(boolean handleNotificationStatus) {
		this.handleNotificationStatus = handleNotificationStatus;
	}

	@Override
	protected void onInit() throws Exception {
		super.onInit();
		if (this.payloadExpression != null) {
			this.evaluationContext = createEvaluationContext();
		}
	}

	@Override
	public String getComponentType() {
		return "aws:sns-inbound-channel-adapter";
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void send(Object object) {
		Message<?> message = (Message<?>) object;
		Map<String, String> payload = (HashMap<String, String>) message.getPayload();
		AbstractIntegrationMessageBuilder<?> messageToSendBuilder;
		if (this.payloadExpression != null) {
			messageToSendBuilder = getMessageBuilderFactory()
					.withPayload(this.payloadExpression.getValue(this.evaluationContext, message))
					.copyHeaders(message.getHeaders());
		}
		else {
			messageToSendBuilder = getMessageBuilderFactory().fromMessage(message);
		}

		String type = payload.get("Type");
		if ("SubscriptionConfirmation".equals(type) || "UnsubscribeConfirmation".equals(type)) {
			JsonNode content = this.jackson2HttpMessageConverter.getObjectMapper().valueToTree(payload);
			NotificationStatus notificationStatus = this.notificationStatusResolver.resolveNotificationStatus(content);
			if (this.handleNotificationStatus) {
				messageToSendBuilder.setHeader(AwsHeaders.NOTIFICATION_STATUS, notificationStatus);
			}
			else {
				if ("SubscriptionConfirmation".equals(type)) {
					notificationStatus.confirmSubscription();
				}
				return;
			}
		}
		messageToSendBuilder.setHeader(AwsHeaders.SNS_MESSAGE_TYPE, type)
				.setHeader(AwsHeaders.MESSAGE_ID, payload.get("MessageId"));
		super.send(messageToSendBuilder.build());
	}

	@Override
	public void setPayloadExpression(Expression payloadExpression) {
		this.payloadExpression = payloadExpression;
	}

	@Override
	public void setHeaderExpressions(Map<String, Expression> headerExpressions) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setMessageConverters(List<HttpMessageConverter<?>> messageConverters) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setMergeWithDefaultConverters(boolean mergeWithDefaultConverters) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setHeaderMapper(HeaderMapper<HttpHeaders> headerMapper) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setRequestMapping(RequestMapping requestMapping) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setRequestPayloadType(Class<?> requestPayloadType) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setExtractReplyPayload(boolean extractReplyPayload) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setMultipartResolver(MultipartResolver multipartResolver) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setStatusCodeExpression(Expression statusCodeExpression) {
		throw new UnsupportedOperationException();
	}

	private static class NotificationStatusResolver extends NotificationStatusHandlerMethodArgumentResolver {

		NotificationStatusResolver(AmazonSNS amazonSns) {
			super(amazonSns);
		}

		NotificationStatus resolveNotificationStatus(JsonNode content) {
			return (NotificationStatus) doResolveArgumentFromNotificationMessage(content, null, null);
		}

	}

}
