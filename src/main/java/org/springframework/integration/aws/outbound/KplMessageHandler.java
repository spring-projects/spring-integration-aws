/*
 * Copyright 2019-2024 the original author or authors.
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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecord;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import org.springframework.context.Lifecycle;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.serializer.support.SerializingConverter;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.aws.support.KPLBackpressureException;
import org.springframework.integration.aws.support.UserRecordResponse;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.mapping.HeaderMapper;
import org.springframework.integration.mapping.OutboundMessageMapper;
import org.springframework.integration.support.MutableMessage;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * The {@link AbstractMessageHandler} implementation for the Amazon Kinesis Producer
 * Library {@code putRecord(s)}.
 *
 * @author Arnaud Lecollaire
 * @author Artem Bilan
 * @author Siddharth Jain
 *
 * @since 2.2
 *
 * @see KinesisAsyncClient#putRecord(PutRecordRequest)
 * @see KinesisAsyncClient#putRecords(PutRecordsRequest)
 * @see com.amazonaws.handlers.AsyncHandler
 */
public class KplMessageHandler extends AbstractAwsMessageHandler<Void> implements Lifecycle {

	private final KinesisProducer kinesisProducer;

	private MessageConverter messageConverter = new ConvertingFromMessageConverter(new SerializingConverter());

	private Expression streamExpression;

	private Expression partitionKeyExpression;

	private Expression explicitHashKeyExpression;

	private Expression sequenceNumberExpression;

	private Expression glueSchemaExpression;

	private OutboundMessageMapper<byte[]> embeddedHeadersMapper;

	private Duration flushDuration = Duration.ofMillis(0);

	private volatile boolean running;

	private volatile ScheduledFuture<?> flushFuture;

	private long backPressureThreshold = 0;

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
	 * Configure maximum records in flight for handling backpressure. By Default, backpressure handling is not enabled.
	 * On number of records flights exceeding the threshold, {@link KPLBackpressureException} would be thrown.
	 * If Backpressure handling is enabled, {@link KPLBackpressureException} must be handled.
	 * @param backPressureThreshold Defaulted to 0. Set a value greater than 0 to enable backpressure handling.
	 * @since 3.0.9
	 */
	public void setBackPressureThreshold(long backPressureThreshold) {
		Assert.isTrue(backPressureThreshold > 0, "'maxInFlightRecords must be greater than 0.");
		this.backPressureThreshold = backPressureThreshold;
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
	 * Configure a {@link Duration} how often to call a {@link KinesisProducer#flush()}.
	 * @param flushDuration the {@link Duration} to periodic call of a {@link KinesisProducer#flush()}.
	 * @since 2.3.6
	 */
	public void setFlushDuration(Duration flushDuration) {
		Assert.notNull(flushDuration, "'flushDuration' must not be null.");
		this.flushDuration = flushDuration;
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

	/**
	 * Set a {@link Schema} to add into a {@link UserRecord} built from the request message.
	 * @param glueSchema the {@link Schema} to add into a {@link UserRecord}.
	 * @since 2.5.2
	 * @see UserRecord#setSchema(Schema)
	 */
	public void setGlueSchema(Schema glueSchema) {
		setPartitionKeyExpression(new ValueExpression<>(glueSchema));
	}

	/**
	 * Set a SpEL expression for {@link Schema} to add into a {@link UserRecord}
	 * built from the request message.
	 * @param glueSchemaExpression the SpEL expression to evaluate a {@link Schema}.
	 * @since 2.5.2
	 * @see UserRecord#setSchema(Schema)
	 */
	public void setGlueSchemaExpressionString(String glueSchemaExpression) {
		setGlueSchemaExpression(EXPRESSION_PARSER.parseExpression(glueSchemaExpression));
	}

	/**
	 * Set a SpEL expression for {@link Schema} to add into a {@link UserRecord}
	 * built from the request message.
	 * @param glueSchemaExpression the SpEL expression to evaluate a {@link Schema}.
	 * @since 2.5.2
	 * @see UserRecord#setSchema(Schema)
	 */
	public void setGlueSchemaExpression(Expression glueSchemaExpression) {
		this.glueSchemaExpression = glueSchemaExpression;
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
	protected AwsRequest messageToAwsRequest(Message<?> message) {
		Object payload = message.getPayload();
		if (payload instanceof PutRecordsRequest) {
			return (PutRecordsRequest) payload;
		}
		else if (payload instanceof PutRecordRequest) {
			return (PutRecordRequest) payload;
		}
		else if (payload instanceof UserRecord) {
			return buildPutRecordRequest(message);
		}

		return buildPutRecordRequest(message);
	}

	@Override
	protected CompletableFuture<? extends AwsResponse> handleMessageToAws(Message<?> message, AwsRequest request) {
		try {
			if (request instanceof PutRecordsRequest putRecordsRequest) {
				return handlePutRecordsRequest(message, putRecordsRequest);
			}
			else if (message.getPayload() instanceof UserRecord userRecord) {
				return handleUserRecord(userRecord);
			}

			PutRecordRequest putRecordRequest = (PutRecordRequest) request;
			// convert the PutRecordRequest to a UserRecord
			UserRecord userRecord = new UserRecord();
			userRecord.setExplicitHashKey(putRecordRequest.explicitHashKey());
			userRecord.setData(putRecordRequest.data().asByteBuffer());
			userRecord.setPartitionKey(putRecordRequest.partitionKey());
			userRecord.setStreamName(putRecordRequest.streamName());
			setGlueSchemaIntoUserRecordIfAny(userRecord, message);
			return handleUserRecord(userRecord);
		}
		finally {
			if (this.flushDuration.toMillis() <= 0) {
				this.kinesisProducer.flush();
			}
		}
	}

	@Override
	protected Map<String, ?> additionalOnSuccessHeaders(AwsRequest request, AwsResponse response) {
		if (response instanceof UserRecordResponse putRecordResponse) {
			return Map.of(AwsHeaders.SHARD, putRecordResponse.shardId(),
					AwsHeaders.SEQUENCE_NUMBER, putRecordResponse.sequenceNumber());
		}
		return null;
	}

	private CompletableFuture<PutRecordsResponse> handlePutRecordsRequest(Message<?> message,
			PutRecordsRequest putRecordsRequest) {

		AtomicInteger failedRecordsCount = new AtomicInteger();

		return Flux.fromIterable(putRecordsRequest.records())
				.map((putRecordsRequestEntry) -> {
					UserRecord userRecord = new UserRecord();
					userRecord.setExplicitHashKey(putRecordsRequestEntry.explicitHashKey());
					userRecord.setData(putRecordsRequestEntry.data().asByteBuffer());
					userRecord.setPartitionKey(putRecordsRequestEntry.partitionKey());
					userRecord.setStreamName(putRecordsRequest.streamName());
					setGlueSchemaIntoUserRecordIfAny(userRecord, message);
					return userRecord;
				})
				.concatMap((userRecord) ->
						Mono.fromFuture(handleUserRecord(userRecord))
								.map(recordResult ->
										PutRecordsResultEntry.builder()
												.sequenceNumber(recordResult.sequenceNumber())
												.shardId(recordResult.shardId())
												.build())
								.onErrorResume(UserRecordFailedException.class,
										(ex) -> Mono.just(ex.getResult())
												.map((errorRecord) -> {
													PutRecordsResultEntry.Builder putRecordsResultEntry =
															PutRecordsResultEntry.builder()
																	.sequenceNumber(errorRecord.getSequenceNumber())
																	.shardId(errorRecord.getShardId());
													failedRecordsCount.incrementAndGet();
													errorRecord.getAttempts()
															.stream()
															.reduce((left, right) -> right)
															.ifPresent((attempt) ->
																	putRecordsResultEntry
																			.errorMessage(attempt.getErrorMessage())
																			.errorCode(attempt.getErrorCode()));
													return putRecordsResultEntry.build();
												})))
				.collectList()
				.map((putRecordsResultList) ->
						PutRecordsResponse.builder()
								.records(putRecordsResultList)
								.failedRecordCount(failedRecordsCount.get())
								.build())
				.toFuture();
	}

	private void setGlueSchemaIntoUserRecordIfAny(UserRecord userRecord, Message<?> message) {
		if (this.glueSchemaExpression != null) {
			Schema schema = this.glueSchemaExpression.getValue(getEvaluationContext(), message, Schema.class);
			userRecord.setSchema(schema);
		}
	}

	private CompletableFuture<UserRecordResponse> handleUserRecord(UserRecord userRecord)
			throws KPLBackpressureException {

		if (this.backPressureThreshold > 0) {
			var numberOfRecordsInFlight = this.kinesisProducer.getOutstandingRecordsCount();
			if (numberOfRecordsInFlight > this.backPressureThreshold) {
				logger.error(String.format("Backpressure handling is enabled, Number of records in flight: %s is " +
						"greater than backpressure threshold: %s" +
						".", numberOfRecordsInFlight, this.backPressureThreshold));
				throw new KPLBackpressureException("Buffer already at max capacity.");
			}
		}

		ListenableFuture<UserRecordResult> recordResult = this.kinesisProducer.addUserRecord(userRecord);
		return listenableFutureToCompletableFuture(recordResult)
				.thenApply(UserRecordResponse::new);
	}

	private PutRecordRequest buildPutRecordRequest(Message<?> message) {
		Object payload = message.getPayload();

		ByteBuffer data = null;
		String sequenceNumber = null;
		String stream;
		String partitionKey;
		String explicitHashKey;

		if (payload instanceof UserRecord userRecord) {
			data = userRecord.getData();
			stream = userRecord.getStreamName();
			partitionKey = userRecord.getPartitionKey();
			explicitHashKey = userRecord.getExplicitHashKey();
		}
		else {
			MessageHeaders messageHeaders = message.getHeaders();
			stream = messageHeaders.get(AwsHeaders.STREAM, String.class);
			if (!StringUtils.hasText(stream) && this.streamExpression != null) {
				stream = this.streamExpression.getValue(getEvaluationContext(), message, String.class);
			}
			Assert.state(stream != null,
					"'stream' must not be null for sending a Kinesis record. "
							+ "Consider configuring this handler with a 'stream'( or 'streamExpression') or supply an "
							+ "'aws_stream' message header.");

			partitionKey = messageHeaders.get(AwsHeaders.PARTITION_KEY, String.class);
			if (!StringUtils.hasText(partitionKey) && this.partitionKeyExpression != null) {
				partitionKey = this.partitionKeyExpression.getValue(getEvaluationContext(), message, String.class);
			}
			Assert.state(partitionKey != null,
					"'partitionKey' must not be null for sending a Kinesis record. "
					+ "Consider configuring this handler with a 'partitionKey'( or 'partitionKeyExpression') " +
					"or supply an 'aws_partitionKey' message header.");

			explicitHashKey = (this.explicitHashKeyExpression != null
					? this.explicitHashKeyExpression.getValue(getEvaluationContext(), message, String.class) : null);

			sequenceNumber = messageHeaders.get(AwsHeaders.SEQUENCE_NUMBER, String.class);
			if (!StringUtils.hasText(sequenceNumber) && this.sequenceNumberExpression != null) {
				sequenceNumber = this.sequenceNumberExpression.getValue(getEvaluationContext(), message, String.class);
			}

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
		}

		return PutRecordRequest.builder()
				.streamName(stream)
				.partitionKey(partitionKey)
				.explicitHashKey(explicitHashKey)
				.sequenceNumberForOrdering(sequenceNumber)
				.data(SdkBytes.fromByteBuffer(data))
				.build();
	}

	private static <T> CompletableFuture<T> listenableFutureToCompletableFuture(ListenableFuture<T> listenableFuture) {
		CompletableFuture<T> completable = new CompletableFuture<>() {

			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				// propagate cancel to the listenable future
				boolean result = listenableFuture.cancel(mayInterruptIfRunning);
				super.cancel(mayInterruptIfRunning);
				return result;
			}

		};

		// add callback
		Futures.addCallback(listenableFuture, new FutureCallback<>() {

			@Override
			public void onSuccess(T result) {
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
