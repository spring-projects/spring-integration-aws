/*
 * Copyright 2017-2025 the original author or authors.
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.amazonaws.handlers.AsyncHandler;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.integration.MessageTimeoutException;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.aws.support.AwsRequestFailureException;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.handler.AbstractMessageProducingHandler;
import org.springframework.integration.mapping.HeaderMapper;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * The base {@link AbstractMessageProducingHandler} for AWS services. Utilizes common
 * logic ({@link AsyncHandler}, {@link ErrorMessageStrategy}, {@code failureChannel} etc.)
 * and message pre- and post-processing,
 *
 * @param <H> the headers container type.
 *
 * @author Artem Bilan
 *
 * @since 2.0
 */
public abstract class AbstractAwsMessageHandler<H> extends AbstractMessageProducingHandler {

	protected static final long DEFAULT_SEND_TIMEOUT = 10000;

	private EvaluationContext evaluationContext;

	private Expression sendTimeoutExpression = new ValueExpression<>(DEFAULT_SEND_TIMEOUT);

	private HeaderMapper<H> headerMapper;

	private boolean headerMapperSet;

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
	 * Specify a {@link HeaderMapper} to map outbound headers.
	 * @param headerMapper the {@link HeaderMapper} to map outbound headers.
	 */
	public void setHeaderMapper(HeaderMapper<H> headerMapper) {
		this.headerMapper = headerMapper;
		this.headerMapperSet = true;
	}

	protected boolean isHeaderMapperSet() {
		return this.headerMapperSet;
	}

	/**
	 * Set a {@link HeaderMapper} to use.
	 * @param headerMapper the header mapper to set
	 * @deprecated in favor of {@link #setHeaderMapper(HeaderMapper)} to be called from {@link #onInit()}.
	 */
	@Deprecated(forRemoval = true, since = "3.0.8")
	protected void doSetHeaderMapper(HeaderMapper<H> headerMapper) {
		this.headerMapper = headerMapper;
	}

	protected HeaderMapper<H> getHeaderMapper() {
		return this.headerMapper;
	}

	protected EvaluationContext getEvaluationContext() {
		return this.evaluationContext;
	}

	@Override
	protected void onInit() {
		super.onInit();
		this.evaluationContext = ExpressionUtils.createStandardEvaluationContext(getBeanFactory());
	}

	@Override
	protected boolean shouldCopyRequestHeaders() {
		return false;
	}

	@Override
	protected void handleMessageInternal(Message<?> message) {
		AwsRequest request = messageToAwsRequest(message);
		CompletableFuture<?> resultFuture =
				handleMessageToAws(message, request)
						.handle((response, ex) -> handleResponse(message, request, response, ex));

		if (isAsync()) {
			sendOutputs(resultFuture, message);
			return;
		}

		Long sendTimeout = this.sendTimeoutExpression.getValue(this.evaluationContext, message, Long.class);
		if (sendTimeout == null || sendTimeout < 0) {
			try {
				resultFuture.get();
			}
			catch (InterruptedException | ExecutionException ex) {
				throw new IllegalStateException(ex);
			}
		}
		else {
			try {
				resultFuture.get(sendTimeout, TimeUnit.MILLISECONDS);
			}
			catch (TimeoutException te) {
				throw new MessageTimeoutException(message, "Timeout waiting for response from AmazonKinesis", te);
			}
			catch (InterruptedException | ExecutionException ex) {
				throw new IllegalStateException(ex);
			}
		}
	}

	protected Message<?> handleResponse(Message<?> message, AwsRequest request, AwsResponse response, Throwable cause) {
		if (cause != null) {
			throw new AwsRequestFailureException(message, request, cause);
		}
		return getMessageBuilderFactory()
				.fromMessage(message)
				.copyHeadersIfAbsent(additionalOnSuccessHeaders(request, response))
				.setHeaderIfAbsent(AwsHeaders.SERVICE_RESULT, response)
				.build();
	}

	protected abstract AwsRequest messageToAwsRequest(Message<?> message);

	protected abstract CompletableFuture<? extends AwsResponse> handleMessageToAws(Message<?> message,
			AwsRequest request);

	@Nullable
	protected abstract Map<String, ?> additionalOnSuccessHeaders(AwsRequest request, AwsResponse response);

}
