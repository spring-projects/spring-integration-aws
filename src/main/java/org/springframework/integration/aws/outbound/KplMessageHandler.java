/*
 * Copyright 2019 the original author or authors.
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

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.checkerframework.checker.nullness.qual.Nullable;

import org.springframework.context.Lifecycle;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.serializer.support.SerializingConverter;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.aws.support.AwsRequestFailureException;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.mapping.HeaderMapper;
import org.springframework.integration.mapping.OutboundMessageMapper;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.integration.support.MutableMessage;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecord;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The {@link AbstractMessageHandler} implementation for the Amazon Kinesis Producer
 * Library {@code putRecord(s)}.
 *
 * @author Arnaud Lecollaire
 * @author Artem Bilan
 *
 * @since 2.2
 *
 * @see AmazonKinesisAsync#putRecord(PutRecordRequest)
 * @see AmazonKinesisAsync#putRecords(PutRecordsRequest)
 * @see com.amazonaws.handlers.AsyncHandler
 */
public class KplMessageHandler extends AbstractAwsMessageHandler<Void> implements Lifecycle {

	private final KinesisProducer kinesisProducer;

	private MessageConverter messageConverter = new ConvertingFromMessageConverter(new SerializingConverter());

	private Expression streamExpression;

	private Expression partitionKeyExpression;

	private Expression explicitHashKeyExpression;

	private Expression sequenceNumberExpression;

	private OutboundMessageMapper<byte[]> embeddedHeadersMapper;

	private Duration flushDuration = Duration.ofMillis(0);

	private volatile boolean running;

	private volatile ScheduledFuture<?> flushFuture;

	public KplMessageHandler(KinesisProducer kinesisProducer) {
		Assert.notNull(kinesisProducer, "'kinesisProducer' must not be null.");
		this.kinesisProducer = kinesisProducer;
	}

	/**
	 * Specify a {@link Converter} to serialize {@code payload} to the {@code byte[]} if
	 * that isn't {@code byte[]} already.
	 * @param converter the {@link Converter} to use; cannot be null.
	 @deprecated since 2.3 in favor of {@link #setMessageConverter}
	 */
	@Deprecated
	public void setConverter(Converter<Object, byte[]> converter) {
		setMessageConverter(new ConvertingFromMessageConverter(converter));
	}

	/**
	 * Configure a {@link MessageConverter} for converting payload to {@code byte[]} for Kinesis record.
	 * @param messageConverter the {@link MessageConverter} to use.
	 * @since 2.3
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		Assert.notNull(messageConverter, "'messageConverter' must not be null.");
		this.messageConverter = messageConverter;
	}

	public void setStream(String stream) {
		setStreamExpression(new LiteralExpression(stream));
	}

	public void setStreamExpressionString(String streamExpression) {
		setStreamExpression(EXPRESSION_PARSER.parseExpression(streamExpression));
	}

	public void setStreamExpression(Expression streamExpression) {
		this.streamExpression = streamExpression;
	}

	public void setPartitionKey(String partitionKey) {
		setPartitionKeyExpression(new LiteralExpression(partitionKey));
	}

	public void setPartitionKeyExpressionString(String partitionKeyExpression) {
		setPartitionKeyExpression(EXPRESSION_PARSER.parseExpression(partitionKeyExpression));
	}

	public void setPartitionKeyExpression(Expression partitionKeyExpression) {
		this.partitionKeyExpression = partitionKeyExpression;
	}

	public void setExplicitHashKey(String explicitHashKey) {
		setExplicitHashKeyExpression(new LiteralExpression(explicitHashKey));
	}

	public void setExplicitHashKeyExpressionString(String explicitHashKeyExpression) {
		setExplicitHashKeyExpression(EXPRESSION_PARSER.parseExpression(explicitHashKeyExpression));
	}

	public void setExplicitHashKeyExpression(Expression explicitHashKeyExpression) {
		this.explicitHashKeyExpression = explicitHashKeyExpression;
	}

	public void setSequenceNumberExpressionString(String sequenceNumberExpression) {
		setSequenceNumberExpression(EXPRESSION_PARSER.parseExpression(sequenceNumberExpression));
	}

	public void setSequenceNumberExpression(Expression sequenceNumberExpression) {
		this.sequenceNumberExpression = sequenceNumberExpression;
	}

	/**
	 * Specify a {@link OutboundMessageMapper} for embedding message headers into the
	 * record data together with payload.
	 * @param embeddedHeadersMapper the {@link OutboundMessageMapper} to embed headers
	 * into the record data.
	 * @since 2.0
	 * @see org.springframework.integration.support.json.EmbeddedJsonHeadersMessageMapper
	 */
	public void setEmbeddedHeadersMapper(OutboundMessageMapper<byte[]> embeddedHeadersMapper) {
		this.embeddedHeadersMapper = embeddedHeadersMapper;
	}

	/**
	 * Unsupported operation. Use {@link #setEmbeddedHeadersMapper} instead.
	 * @param headerMapper is not used.
	 * @see #setEmbeddedHeadersMapper
	 */
	@Override
	public void setHeaderMapper(HeaderMapper<Void> headerMapper) {
		throw new UnsupportedOperationException("Kinesis doesn't support headers.\n"
				+ "Consider to use 'OutboundMessageMapper<byte[]>' for embedding headers into the record data.");
	}


	@Override
	public synchronized void start() {
		if (!this.running) {
			if (this.flushDuration.toMillis() > 0) {
				this.flushFuture = getTaskScheduler()
						.scheduleAtFixedRate(this.kinesisProducer::flush, this.flushDuration);
			}
			this.running = true;
		}
	}

	@Override
	public synchronized void stop() {
		if (this.running) {
			this.running = false;
			if (this.flushFuture != null) {
				this.flushFuture.cancel(true);
			}
		}
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	protected Future<?> handleMessageToAws(Message<?> message) {
		try {
			if (message.getPayload() instanceof PutRecordsRequest) {
				return handlePutRecordsRequest(message, (PutRecordsRequest) message.getPayload());
			}
			else if (message.getPayload() instanceof UserRecord) {
				return handleUserRecord(message, buildPutRecordRequest(message), (UserRecord) message.getPayload());
			}
			else {
				final PutRecordRequest putRecordRequest = (message.getPayload() instanceof PutRecordRequest)
						? (PutRecordRequest) message.getPayload() : buildPutRecordRequest(message);

				// convert the PutRecordRequest to a UserRecord
				UserRecord userRecord = new UserRecord();
				userRecord.setExplicitHashKey(putRecordRequest.getExplicitHashKey());
				userRecord.setData(putRecordRequest.getData());
				userRecord.setPartitionKey(putRecordRequest.getPartitionKey());
				userRecord.setStreamName(putRecordRequest.getStreamName());
				return handleUserRecord(message, putRecordRequest, userRecord);
			}
		}
		finally {
			if (this.flushDuration.toMillis() <= 0) {
				this.kinesisProducer.flush();
			}
		}
	}

	private Future<PutRecordsResult> handlePutRecordsRequest(Message<?> message, PutRecordsRequest putRecordsRequest) {
		PutRecordsResult putRecordsResult = new PutRecordsResult();
		SettableFuture<PutRecordsResult> putRecordsResultFuture = SettableFuture.create();
		AtomicInteger failedRecordsCount = new AtomicInteger();
		Flux.fromIterable(putRecordsRequest.getRecords())
				.map((putRecordsRequestEntry) -> {
					UserRecord userRecord = new UserRecord();
					userRecord.setExplicitHashKey(putRecordsRequestEntry.getExplicitHashKey());
					userRecord.setData(putRecordsRequestEntry.getData());
					userRecord.setPartitionKey(putRecordsRequestEntry.getPartitionKey());
					userRecord.setStreamName(putRecordsRequest.getStreamName());
					return userRecord;
				})
				.concatMap((userRecord) ->
						Mono.fromFuture(listenableFutureToCompletableFuture(
								this.kinesisProducer.addUserRecord(userRecord))))
				.map((userRecordResult) -> {
					PutRecordsResultEntry putRecordsResultEntry =
							new PutRecordsResultEntry()
									.withSequenceNumber(userRecordResult.getSequenceNumber())
									.withShardId(userRecordResult.getShardId());

					if (!userRecordResult.isSuccessful()) {
						failedRecordsCount.incrementAndGet();
						userRecordResult.getAttempts()
								.stream()
								.reduce((left, right) -> right)
								.ifPresent((attempt) ->
										putRecordsResultEntry
												.withErrorMessage(attempt.getErrorMessage())
												.withErrorCode(attempt.getErrorCode()));
					}

					return putRecordsResultEntry;
				})
				.collectList()
				.map((putRecordsResultList) ->
						putRecordsResult.withRecords(putRecordsResultList)
								.withFailedRecordCount(failedRecordsCount.get()))
				.subscribe(putRecordsResultFuture::set, putRecordsResultFuture::setException);

		applyCallbackForAsyncHandler(message, putRecordsRequest, putRecordsResultFuture);

		return putRecordsResultFuture;
	}

	private Future<?> handleUserRecord(Message<?> message, PutRecordRequest putRecordRequest, UserRecord userRecord) {
		ListenableFuture<UserRecordResult> recordResult = this.kinesisProducer.addUserRecord(userRecord);
		applyCallbackForAsyncHandler(message, putRecordRequest, recordResult);
		return recordResult;
	}

	private <R> void applyCallbackForAsyncHandler(Message<?> message, AmazonWebServiceRequest serviceRequest,
			ListenableFuture<R> result) {

		AsyncHandler<AmazonWebServiceRequest, R> asyncHandler = obtainAsyncHandler(message, serviceRequest);
		FutureCallback<R> callback =
				new FutureCallback<R>() {

					@Override
					public void onFailure(Throwable ex) {
						asyncHandler.onError(ex instanceof Exception ? (Exception) ex
								: new AwsRequestFailureException(message, serviceRequest, ex));
					}

					@Override
					public void onSuccess(R result) {
						asyncHandler.onSuccess(serviceRequest, result);
					}

				};

		Futures.addCallback(result, callback, MoreExecutors.directExecutor());
	}

	private PutRecordRequest buildPutRecordRequest(Message<?> message) {
		MessageHeaders messageHeaders = message.getHeaders();
		String stream = messageHeaders.get(AwsHeaders.STREAM, String.class);
		if (!StringUtils.hasText(stream) && this.streamExpression != null) {
			stream = this.streamExpression.getValue(getEvaluationContext(), message, String.class);
		}
		Assert.state(stream != null,
				"'stream' must not be null for sending a Kinesis record. "
						+ "Consider configuring this handler with a 'stream'( or 'streamExpression') or supply an "
						+ "'aws_stream' message header.");

		String partitionKey = messageHeaders.get(AwsHeaders.PARTITION_KEY, String.class);
		if (!StringUtils.hasText(partitionKey) && this.partitionKeyExpression != null) {
			partitionKey = this.partitionKeyExpression.getValue(getEvaluationContext(), message, String.class);
		}
		Assert.state(partitionKey != null, "'partitionKey' must not be null for sending a Kinesis record. "
				+ "Consider configuring this handler with a 'partitionKey'( or 'partitionKeyExpression') or supply an "
				+ "'aws_partitionKey' message header.");

		String explicitHashKey = (this.explicitHashKeyExpression != null
				? this.explicitHashKeyExpression.getValue(getEvaluationContext(), message, String.class) : null);

		String sequenceNumber = messageHeaders.get(AwsHeaders.SEQUENCE_NUMBER, String.class);
		if (!StringUtils.hasText(sequenceNumber) && this.sequenceNumberExpression != null) {
			sequenceNumber = this.sequenceNumberExpression.getValue(getEvaluationContext(), message, String.class);
		}

		Object payload = message.getPayload();

		ByteBuffer data = null;

		Message<?> messageToEmbed = null;

		if (payload instanceof ByteBuffer) {
			data = (ByteBuffer) payload;
			if (this.embeddedHeadersMapper != null) {
				messageToEmbed = new MutableMessage<>(data.array(), messageHeaders);
			}
		}
		else {
			byte[] bytes =
					(byte[]) (payload instanceof byte[]
							? payload
							: this.messageConverter.fromMessage(message, byte[].class));
			Assert.notNull(bytes, "payload cannot be null");
			if (this.embeddedHeadersMapper != null) {
				messageToEmbed = new MutableMessage<>(bytes, messageHeaders);
			}
			else {
				data = ByteBuffer.wrap(bytes);
			}
		}

		if (messageToEmbed != null) {
			try {
				byte[] bytes = this.embeddedHeadersMapper.fromMessage(messageToEmbed);
				Assert.notNull(bytes, "payload cannot be null");
				data = ByteBuffer.wrap(bytes);
			}
			catch (Exception ex) {
				throw new MessageConversionException(message, "Cannot embedded headers to payload", ex);
			}
		}

		return new PutRecordRequest().withStreamName(stream).withPartitionKey(partitionKey)
				.withExplicitHashKey(explicitHashKey).withSequenceNumberForOrdering(sequenceNumber).withData(data);
	}

	@Override
	protected void additionalOnSuccessHeaders(AbstractIntegrationMessageBuilder<?> messageBuilder,
			AmazonWebServiceRequest request, Object result) {

		if (result instanceof PutRecordResult) {
			messageBuilder.setHeader(AwsHeaders.SHARD, ((PutRecordResult) result).getShardId())
					.setHeader(AwsHeaders.SEQUENCE_NUMBER, ((PutRecordResult) result).getSequenceNumber());
		}
	}

	private static <T> CompletableFuture<T> listenableFutureToCompletableFuture(ListenableFuture<T> listenableFuture) {
		CompletableFuture<T> completable = new CompletableFuture<T>() {

			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				// propagate cancel to the listenable future
				boolean result = listenableFuture.cancel(mayInterruptIfRunning);
				super.cancel(mayInterruptIfRunning);
				return result;
			}

		};

		// add callback
		Futures.addCallback(listenableFuture, new FutureCallback<T>() {

			@Override
			public void onSuccess(@Nullable T result) {
				completable.complete(result);
			}

			@Override
			public void onFailure(Throwable ex) {
				completable.completeExceptionally(ex);
			}
		}, MoreExecutors.directExecutor());

		return completable;
	}

}
