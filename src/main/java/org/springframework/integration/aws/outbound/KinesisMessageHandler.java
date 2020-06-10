/*
 * Copyright 2017-2020 the original author or authors.
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
import java.util.concurrent.Future;

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.serializer.support.SerializingConverter;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.aws.support.AwsHeaders;
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

/**
 * The {@link AbstractMessageHandler} implementation for the Amazon Kinesis
 * {@code putRecord(s)}.
 *
 * @author Artem Bilan
 * @author Jacob Severson
 *
 * @since 1.1
 *
 * @see AmazonKinesisAsync#putRecord(PutRecordRequest)
 * @see AmazonKinesisAsync#putRecords(PutRecordsRequest)
 * @see com.amazonaws.handlers.AsyncHandler
 */
public class KinesisMessageHandler extends AbstractAwsMessageHandler<Void> {

	private final AmazonKinesisAsync amazonKinesis;

	private MessageConverter messageConverter = new ConvertingFromMessageConverter(new SerializingConverter());

	private Expression streamExpression;

	private Expression partitionKeyExpression;

	private Expression explicitHashKeyExpression;

	private Expression sequenceNumberExpression;

	private OutboundMessageMapper<byte[]> embeddedHeadersMapper;

	public KinesisMessageHandler(AmazonKinesisAsync amazonKinesis) {
		Assert.notNull(amazonKinesis, "'amazonKinesis' must not be null.");
		this.amazonKinesis = amazonKinesis;
	}

	/**
	 * Specify a {@link Converter} to serialize {@code payload} to the {@code byte[]} if
	 * that isn't {@code byte[]} already.
	 * @param converter the {@link Converter} to use; cannot be null.
	 * @deprecated since 2.3 in favor of {@link #setMessageConverter}
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
	protected Future<?> handleMessageToAws(Message<?> message) {
		if (message.getPayload() instanceof PutRecordsRequest) {
			AsyncHandler<PutRecordsRequest, PutRecordsResult> asyncHandler = obtainAsyncHandler(message,
					(PutRecordsRequest) message.getPayload());

			return this.amazonKinesis.putRecordsAsync((PutRecordsRequest) message.getPayload(), asyncHandler);
		}
		else {
			final PutRecordRequest putRecordRequest = (message.getPayload() instanceof PutRecordRequest)
					? (PutRecordRequest) message.getPayload() : buildPutRecordRequest(message);

			AsyncHandler<PutRecordRequest, PutRecordResult> asyncHandler = obtainAsyncHandler(message,
					putRecordRequest);

			return this.amazonKinesis.putRecordAsync(putRecordRequest, asyncHandler);
		}
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

}
