/*
 * Copyright 2016-present the original author or authors.
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

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

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
class KinesisMessageHandlerTests {

	@Autowired
	protected KinesisAsyncClient amazonKinesis;

	@Autowired
	protected MessageChannel kinesisSendChannel;

	@Autowired
	protected KinesisMessageHandler kinesisMessageHandler;

	@Test
	@SuppressWarnings("unchecked")
	void kinesisMessageHandler() {
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

		verify(this.amazonKinesis).putRecord(putRecordRequestArgumentCaptor.capture());

		PutRecordRequest putRecordRequest = putRecordRequestArgumentCaptor.getValue();

		assertThat(putRecordRequest.streamName()).isEqualTo("foo");
		assertThat(putRecordRequest.partitionKey()).isEqualTo("fooKey");
		assertThat(putRecordRequest.sequenceNumberForOrdering()).isEqualTo("10");
		assertThat(putRecordRequest.explicitHashKey()).isNull();

		Message<?> messageToCheck = new EmbeddedJsonHeadersMessageMapper()
				.toMessage(putRecordRequest.data().asByteArray());

		assertThat(messageToCheck.getHeaders()).contains(entry("foo", "bar"));
		assertThat(messageToCheck.getPayload()).isEqualTo("message".getBytes());

		message2 = new GenericMessage<>(PutRecordsRequest.builder()
				.streamName("myStream").records(request ->
						request.data(SdkBytes.fromByteArray("test".getBytes()))
								.partitionKey("testKey"))
				.build());

		this.kinesisSendChannel.send(message2);

		ArgumentCaptor<PutRecordsRequest> putRecordsRequestArgumentCaptor = ArgumentCaptor
				.forClass(PutRecordsRequest.class);
		verify(this.amazonKinesis).putRecords(putRecordsRequestArgumentCaptor.capture());

		PutRecordsRequest putRecordsRequest = putRecordsRequestArgumentCaptor.getValue();

		assertThat(putRecordsRequest.streamName()).isEqualTo("myStream");
		assertThat(putRecordsRequest.records())
				.containsExactlyInAnyOrder(
						PutRecordsRequestEntry.builder()
								.data(SdkBytes.fromByteArray("test".getBytes()))
								.partitionKey("testKey")
								.build());
	}

	@Configuration
	@EnableIntegration
	public static class ContextConfiguration {

		@Bean
		@SuppressWarnings("unchecked")
		public KinesisAsyncClient amazonKinesis() {
			KinesisAsyncClient mock = mock(KinesisAsyncClient.class);

			given(mock.putRecord(any(PutRecordRequest.class)))
					.willReturn(mock(CompletableFuture.class));

			given(mock.putRecords(any(PutRecordsRequest.class)))
					.willReturn(mock(CompletableFuture.class));

			return mock;
		}

		@Bean
		@ServiceActivator(inputChannel = "kinesisSendChannel")
		public MessageHandler kinesisMessageHandler() {
			KinesisMessageHandler kinesisMessageHandler = new KinesisMessageHandler(amazonKinesis());
			kinesisMessageHandler.setAsync(true);
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
