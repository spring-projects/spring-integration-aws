/*
 * Copyright 2017-2018 the original author or authors.
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

package org.springframework.integration.aws.outbound;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.serializer.support.SerializingConverter;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.aws.support.AwsRequestFailureException;
import org.springframework.integration.aws.support.FutureConverter;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.mapping.HeaderMapper;
import org.springframework.integration.mapping.OutboundMessageMapper;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.integration.support.MutableMessage;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecord;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * The {@link AbstractMessageHandler} implementation for the Amazon Kinesis Producer Library {@code putRecord(s)}.
 *
 * @author Artem Bilan
 * @author Jacob Severson
 *
 * @since 2.1.0
 *
 * @see AmazonKinesisAsync#putRecord(PutRecordRequest)
 * @see AmazonKinesisAsync#putRecords(PutRecordsRequest)
 * @see com.amazonaws.handlers.AsyncHandler
 */
public class KplMessageHandler extends AbstractAwsMessageHandler<Void> {

	private final KinesisProducer kinesisProducer;

	private Converter<Object, byte[]> converter = new SerializingConverter();

	private Expression streamExpression;

	private Expression partitionKeyExpression;

	private Expression explicitHashKeyExpression;

	private Expression sequenceNumberExpression;

	private OutboundMessageMapper<byte[]> embeddedHeadersMapper;

	public KplMessageHandler(KinesisProducer kinesisProducer) {
		Assert.notNull(kinesisProducer, "'kinesisProducer' must not be null.");
		this.kinesisProducer = kinesisProducer;
	}

	/**
	 * Specify a {@link Converter} to serialize {@code payload} to the {@code byte[]}
	 * if that isn't {@code byte[]} already.
	 * @param converter the {@link Converter} to use; cannot be null.
	 */
	public void setConverter(Converter<Object, byte[]> converter) {
		Assert.notNull(converter, "'converter' must not be null.");
		this.converter = converter;
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
	 * Specify a {@link OutboundMessageMapper} for embedding message headers into the record data
	 * together with payload.
	 * @param embeddedHeadersMapper the {@link OutboundMessageMapper} to embed headers into the record data.
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
		throw new UnsupportedOperationException("Kinesis doesn't support headers.\n" +
				"Consider to use 'OutboundMessageMapper<byte[]>' for embedding headers into the record data.");
	}

	@Override
	protected Future<?> handleMessageToAws(Message<?> message) throws Exception {
		if (message.getPayload() instanceof PutRecordsRequest) {
			throw new UnsupportedOperationException("not implemented");
		}
		else {
			final PutRecordRequest putRecordRequest =
					(message.getPayload() instanceof PutRecordRequest)
							? (PutRecordRequest) message.getPayload()
							: buildPutRecordRequest(message);

			// convert the PutRecordRequest to a UserRecord
			UserRecord userRecord = new UserRecord();
			userRecord.setExplicitHashKey(putRecordRequest.getExplicitHashKey());
			userRecord.setData(putRecordRequest.getData());
			userRecord.setPartitionKey(putRecordRequest.getPartitionKey());
			userRecord.setStreamName(putRecordRequest.getStreamName());
			return handleUserRecord(message, putRecordRequest, userRecord);
		}
	}

	private Future<?> handleUserRecord(Message<?> message, final PutRecordRequest putRecordRequest,
			UserRecord userRecord) {
		ListenableFuture<UserRecordResult> recordResult = this.kinesisProducer.addUserRecord(userRecord);

		final AsyncHandler<PutRecordRequest, PutRecordResult> asyncHandler =
				obtainAsyncHandler(message, putRecordRequest);
		final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
			@Override
			public void onFailure(Throwable t) {
				asyncHandler.onError((t instanceof Exception) ? ((Exception) t) : new AwsRequestFailureException(message, putRecordRequest, t));
			}

			@Override
			public void onSuccess(UserRecordResult result) {
				PutRecordResult putRecordResult = new PutRecordResult();
				putRecordResult.setSequenceNumber(result.getSequenceNumber());
				putRecordResult.setShardId(result.getShardId());
				asyncHandler.onSuccess(putRecordRequest, putRecordResult);
			}
		};
		Futures.addCallback(recordResult, callback, MoreExecutors.directExecutor());

		return new FutureConverter<>(recordResult, userRecordResult -> {
			PutRecordResult putRecordResult = new PutRecordResult();
			putRecordResult.setSequenceNumber(userRecordResult.getSequenceNumber());
			putRecordResult.setShardId(userRecordResult.getShardId());
			asyncHandler.onSuccess(putRecordRequest, putRecordResult);
			return putRecordResult;
		});
	}

	private PutRecordRequest buildPutRecordRequest(Message<?> message) throws Exception {
		MessageHeaders messageHeaders = message.getHeaders();
		String stream = messageHeaders.get(AwsHeaders.STREAM, String.class);
		if (!StringUtils.hasText(stream) && this.streamExpression != null) {
			stream = this.streamExpression.getValue(getEvaluationContext(), message, String.class);
		}
		Assert.state(stream != null, "'stream' must not be null for sending a Kinesis record. " +
				"Consider configuring this handler with a 'stream'( or 'streamExpression') or supply an " +
				"'aws_stream' message header.");

		String partitionKey = messageHeaders.get(AwsHeaders.PARTITION_KEY, String.class);
		if (!StringUtils.hasText(partitionKey) && this.partitionKeyExpression != null) {
			partitionKey = this.partitionKeyExpression.getValue(getEvaluationContext(), message, String.class);
		}
		Assert.state(partitionKey != null, "'partitionKey' must not be null for sending a Kinesis record. " +
				"Consider configuring this handler with a 'partitionKey'( or 'partitionKeyExpression') or supply an " +
				"'aws_partitionKey' message header.");

		String explicitHashKey =
				(this.explicitHashKeyExpression != null
						? this.explicitHashKeyExpression.getValue(getEvaluationContext(), message, String.class)
						: null);

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
					payload instanceof byte[]
							? (byte[]) payload
							: this.converter.convert(payload);
			if (this.embeddedHeadersMapper != null) {
				messageToEmbed = new MutableMessage<>(bytes, messageHeaders);
			}
			else {
				data = ByteBuffer.wrap(bytes);
			}
		}

		if (messageToEmbed != null) {
			data = ByteBuffer.wrap(this.embeddedHeadersMapper.fromMessage(messageToEmbed));
		}

		return new PutRecordRequest()
				.withStreamName(stream)
				.withPartitionKey(partitionKey)
				.withExplicitHashKey(explicitHashKey)
				.withSequenceNumberForOrdering(sequenceNumber)
				.withData(data);
	}

	@Override
	protected void additionalOnSuccessHeaders(AbstractIntegrationMessageBuilder<?> messageBuilder,
			AmazonWebServiceRequest request, Object result) {

		if (result instanceof PutRecordResult) {
			messageBuilder
					.setHeader(AwsHeaders.SHARD, ((PutRecordResult) result).getShardId())
					.setHeader(AwsHeaders.SEQUENCE_NUMBER, ((PutRecordResult) result).getSequenceNumber());
		}
	}

}
