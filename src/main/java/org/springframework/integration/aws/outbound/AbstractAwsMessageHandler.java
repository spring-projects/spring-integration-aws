/*
 * Copyright 2017-2018 the original author or authors.
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

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.integration.MessageTimeoutException;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.aws.support.AwsRequestFailureException;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.handler.AbstractMessageProducingHandler;
import org.springframework.integration.mapping.HeaderMapper;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.integration.support.DefaultErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.util.Assert;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;

/**
 * The base {@link AbstractMessageProducingHandler} for AWS services.
 * Utilizes common logic ({@link AsyncHandler}, {@link ErrorMessageStrategy},
 * {@code failureChannel} etc.) and message pre- and post-processing,
 *
 * @param <H> the headers container type.
 *
 * @author Artem Bilan
 *
 * @since 2.0
 */
public abstract class AbstractAwsMessageHandler<H> extends AbstractMessageProducingHandler {

	protected static final long DEFAULT_SEND_TIMEOUT = 10000;

	private AsyncHandler<? extends AmazonWebServiceRequest, ?> asyncHandler;

	private EvaluationContext evaluationContext;

	private boolean sync;

	private Expression sendTimeoutExpression = new ValueExpression<>(DEFAULT_SEND_TIMEOUT);

	private ErrorMessageStrategy errorMessageStrategy = new DefaultErrorMessageStrategy();

	private MessageChannel failureChannel;

	private String failureChannelName;

	private HeaderMapper<H> headerMapper;

	public void setAsyncHandler(AsyncHandler<? extends AmazonWebServiceRequest, ?> asyncHandler) {
		this.asyncHandler = asyncHandler;
	}

	protected AsyncHandler<? extends AmazonWebServiceRequest, ?> getAsyncHandler() {
		return this.asyncHandler;
	}

	public void setSync(boolean sync) {
		this.sync = sync;
	}

	protected boolean isSync() {
		return this.sync;
	}

	public void setSendTimeout(long sendTimeout) {
		setSendTimeoutExpression(new ValueExpression<>(sendTimeout));
	}

	public void setSendTimeoutExpressionString(String sendTimeoutExpression) {
		setSendTimeoutExpression(EXPRESSION_PARSER.parseExpression(sendTimeoutExpression));
	}

	public void setSendTimeoutExpression(Expression sendTimeoutExpression) {
		Assert.notNull(sendTimeoutExpression, "'sendTimeoutExpression' must not be null");
		this.sendTimeoutExpression = sendTimeoutExpression;
	}

	protected Expression getSendTimeoutExpression() {
		return this.sendTimeoutExpression;
	}

	/**
	 * Set the failure channel. After a failure on put, an {@link ErrorMessage} will be sent
	 * to this channel with a payload of a {@link AwsRequestFailureException} with the
	 * failed message and cause.
	 * @param failureChannel the failure channel.
	 */
	public void setFailureChannel(MessageChannel failureChannel) {
		this.failureChannel = failureChannel;
	}

	/**
	 * Set the failure channel name. After a failure on put, an {@link ErrorMessage} will be
	 * sent to this channel name with a payload of a {@link AwsRequestFailureException}
	 * with the failed message and cause.
	 * @param failureChannelName the failure channel name.
	 */
	public void setFailureChannelName(String failureChannelName) {
		this.failureChannelName = failureChannelName;
	}

	protected MessageChannel getFailureChannel() {
		if (this.failureChannel != null) {
			return this.failureChannel;

		}
		else if (this.failureChannelName != null) {
			this.failureChannel = getChannelResolver().resolveDestination(this.failureChannelName);
			return this.failureChannel;
		}

		return null;
	}

	public void setErrorMessageStrategy(ErrorMessageStrategy errorMessageStrategy) {
		Assert.notNull(errorMessageStrategy, "'errorMessageStrategy' must not be null");
		this.errorMessageStrategy = errorMessageStrategy;
	}

	protected ErrorMessageStrategy getErrorMessageStrategy() {
		return this.errorMessageStrategy;
	}

	/**
	 * Specify a {@link HeaderMapper} to map outbound headers.
	 * @param headerMapper the {@link HeaderMapper} to map outbound headers.
	 */
	public void setHeaderMapper(HeaderMapper<H> headerMapper) {
		doSetHeaderMapper(headerMapper);
	}

	protected final void doSetHeaderMapper(HeaderMapper<H> headerMapper) {
		this.headerMapper = headerMapper;
	}

	protected HeaderMapper<H> getHeaderMapper() {
		return this.headerMapper;
	}

	protected EvaluationContext getEvaluationContext() {
		return this.evaluationContext;
	}

	@Override
	protected void onInit() throws Exception {
		super.onInit();
		this.evaluationContext = ExpressionUtils.createStandardEvaluationContext(getBeanFactory());
	}

	@Override
	protected void handleMessageInternal(Message<?> message) throws Exception {
		Future<?> resultFuture = handleMessageToAws(message);

		if (this.sync) {
			Long sendTimeout = this.sendTimeoutExpression.getValue(this.evaluationContext, message, Long.class);
			if (sendTimeout == null || sendTimeout < 0) {
				resultFuture.get();
			}
			else {
				try {
					resultFuture.get(sendTimeout, TimeUnit.MILLISECONDS);
				}
				catch (TimeoutException te) {
					throw new MessageTimeoutException(message, "Timeout waiting for response from AmazonKinesis", te);
				}
			}
		}
	}

	protected <I extends AmazonWebServiceRequest, O> AsyncHandler<I, O> obtainAsyncHandler(final Message<?> message,
			final AmazonWebServiceRequest request) {

		return new AsyncHandler<I, O>() {

			@Override
			public void onError(Exception ex) {
				if (getAsyncHandler() != null) {
					getAsyncHandler().onError(ex);
				}

				if (getFailureChannel() != null) {
					AbstractAwsMessageHandler.this.messagingTemplate.send(getFailureChannel(),
							getErrorMessageStrategy()
									.buildErrorMessage(new AwsRequestFailureException(message, request, ex), null));
				}
			}

			@Override
			@SuppressWarnings("unchecked")
			public void onSuccess(I request, O result) {
				if (getAsyncHandler() != null) {
					((AsyncHandler<I, O>) getAsyncHandler()).onSuccess(request, result);
				}

				if (getOutputChannel() != null) {
					AbstractIntegrationMessageBuilder<?> messageBuilder =
							getMessageBuilderFactory()
									.fromMessage(message);

					additionalOnSuccessHeaders(messageBuilder, request, result);

					messageBuilder.setHeaderIfAbsent(AwsHeaders.SERVICE_RESULT, result);


					AbstractAwsMessageHandler.this.messagingTemplate.send(getOutputChannel(), messageBuilder.build());
				}
			}

		};
	}

	protected abstract Future<?> handleMessageToAws(Message<?> message) throws Exception;

	protected abstract void additionalOnSuccessHeaders(AbstractIntegrationMessageBuilder<?> messageBuilder,
			AmazonWebServiceRequest request, Object result);

}
