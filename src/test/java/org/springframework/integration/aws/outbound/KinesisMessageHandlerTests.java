/*
 * Copyright 2016-2022 the original author or authors.
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

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.serializer.support.SerializingConverter;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.support.json.EmbeddedJsonHeadersMessageMapper;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Artem Bilan
 *
 * @since 1.1
 */
@SpringJUnitConfig
@DirtiesContext
public class KinesisMessageHandlerTests {

	@Autowired
	protected AmazonKinesisAsync amazonKinesis;

	@Autowired
	protected MessageChannel kinesisSendChannel;

	@Autowired
	protected KinesisMessageHandler kinesisMessageHandler;

	@Autowired
	protected AsyncHandler<?, ?> asyncHandler;

	@Test
	@SuppressWarnings("unchecked")
	void testKinesisMessageHandler() {
		final Message<?> message = MessageBuilder.withPayload("message").build();

		assertThatExceptionOfType(MessageHandlingException.class)
				.isThrownBy(() -> this.kinesisSendChannel.send(message))
				.withCauseInstanceOf(IllegalStateException.class)
				.withStackTraceContaining("'stream' must not be null for sending a Kinesis record");

		this.kinesisMessageHandler.setStream("foo");

		assertThatExceptionOfType(MessageHandlingException.class)
				.isThrownBy(() -> this.kinesisSendChannel.send(message))
				.withCauseInstanceOf(IllegalStateException.class)
				.withStackTraceContaining("'partitionKey' must not be null for sending a Kinesis record");

		Message<?> message2 = MessageBuilder.fromMessage(message).setHeader(AwsHeaders.PARTITION_KEY, "fooKey")
				.setHeader(AwsHeaders.SEQUENCE_NUMBER, "10").setHeader("foo", "bar").build();

		this.kinesisSendChannel.send(message2);

		ArgumentCaptor<PutRecordRequest> putRecordRequestArgumentCaptor = ArgumentCaptor
				.forClass(PutRecordRequest.class);
		ArgumentCaptor<AsyncHandler<PutRecordRequest, PutRecordResult>> asyncHandlerArgumentCaptor = ArgumentCaptor
				.forClass((Class<AsyncHandler<PutRecordRequest, PutRecordResult>>) (Class<?>) AsyncHandler.class);

		verify(this.amazonKinesis).putRecordAsync(putRecordRequestArgumentCaptor.capture(),
				asyncHandlerArgumentCaptor.capture());

		PutRecordRequest putRecordRequest = putRecordRequestArgumentCaptor.getValue();

		assertThat(putRecordRequest.getStreamName()).isEqualTo("foo");
		assertThat(putRecordRequest.getPartitionKey()).isEqualTo("fooKey");
		assertThat(putRecordRequest.getSequenceNumberForOrdering()).isEqualTo("10");
		assertThat(putRecordRequest.getExplicitHashKey()).isNull();

		Message<?> messageToCheck = new EmbeddedJsonHeadersMessageMapper()
				.toMessage(putRecordRequest.getData().array());

		assertThat(messageToCheck.getHeaders()).contains(entry("foo", "bar"));
		assertThat(messageToCheck.getPayload()).isEqualTo("message".getBytes());

		AsyncHandler<?, ?> asyncHandler = asyncHandlerArgumentCaptor.getValue();

		RuntimeException testingException = new RuntimeException("testingException");
		asyncHandler.onError(testingException);

		verify(this.asyncHandler).onError(eq(testingException));

		message2 = new GenericMessage<>(new PutRecordsRequest().withStreamName("myStream").withRecords(
				new PutRecordsRequestEntry().withData(ByteBuffer.wrap("test".getBytes())).withPartitionKey("testKey")));

		this.kinesisSendChannel.send(message2);

		ArgumentCaptor<PutRecordsRequest> putRecordsRequestArgumentCaptor = ArgumentCaptor
				.forClass(PutRecordsRequest.class);
		verify(this.amazonKinesis).putRecordsAsync(putRecordsRequestArgumentCaptor.capture(), any(AsyncHandler.class));

		PutRecordsRequest putRecordsRequest = putRecordsRequestArgumentCaptor.getValue();

		assertThat(putRecordsRequest.getStreamName()).isEqualTo("myStream");
		assertThat(putRecordsRequest.getRecords()).containsExactlyInAnyOrder(
				new PutRecordsRequestEntry().withData(ByteBuffer.wrap("test".getBytes())).withPartitionKey("testKey"));
	}

	@Configuration
	@EnableIntegration
	public static class ContextConfiguration {

		@Bean
		@SuppressWarnings("unchecked")
		public AmazonKinesisAsync amazonKinesis() {
			AmazonKinesisAsync mock = mock(AmazonKinesisAsync.class);

			given(mock.putRecordAsync(any(PutRecordRequest.class), any(AsyncHandler.class)))
					.willReturn(mock(Future.class));

			given(mock.putRecordsAsync(any(PutRecordsRequest.class), any(AsyncHandler.class)))
					.willReturn(mock(Future.class));

			return mock;
		}

		@Bean
		@SuppressWarnings("unchecked")
		public AsyncHandler<?, ?> asyncHandler() {
			return mock(AsyncHandler.class);
		}

		@Bean
		@ServiceActivator(inputChannel = "kinesisSendChannel")
		public MessageHandler kinesisMessageHandler() {
			KinesisMessageHandler kinesisMessageHandler = new KinesisMessageHandler(amazonKinesis());
			kinesisMessageHandler.setSync(true);
			kinesisMessageHandler.setAsyncHandler(asyncHandler());
			kinesisMessageHandler.setMessageConverter(new MessageConverter() {

				private SerializingConverter serializingConverter = new SerializingConverter();

				@Override
				public Object fromMessage(Message<?> message, Class<?> targetClass) {
					Object source = message.getPayload();
					if (source instanceof String) {
						return ((String) source).getBytes();
					}
					else {
						return this.serializingConverter.convert(source);
					}
				}

				@Override
				public Message<?> toMessage(Object payload, MessageHeaders headers) {
					return null;
				}

			});
			kinesisMessageHandler.setEmbeddedHeadersMapper(new EmbeddedJsonHeadersMessageMapper("foo"));
			return kinesisMessageHandler;
		}

	}

}
