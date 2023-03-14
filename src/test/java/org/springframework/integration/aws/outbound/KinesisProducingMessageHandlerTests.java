/*
 * Copyright 2017-2023 the original author or authors.
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
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.serializer.support.SerializingConverter;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.aws.support.AwsRequestFailureException;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * @author Jacob Severson
 * @author Artem Bilan
 *
 * @since 1.1
 */
@SpringJUnitConfig
@DirtiesContext
public class KinesisProducingMessageHandlerTests {

	@Autowired
	protected MessageChannel kinesisSendChannel;

	@Autowired
	protected KinesisMessageHandler kinesisMessageHandler;

	@Autowired
	protected PollableChannel errorChannel;

	@Autowired
	protected PollableChannel successChannel;

	@Test
	public void testKinesisMessageHandler() {
		final Message<?> message =
				MessageBuilder.withPayload("message")
						.setErrorChannel(this.errorChannel)
						.build();

		assertThatExceptionOfType(MessageHandlingException.class)
				.isThrownBy(() -> this.kinesisSendChannel.send(message))
				.withCauseInstanceOf(IllegalStateException.class)
				.withStackTraceContaining("'stream' must not be null for sending a Kinesis record");

		this.kinesisMessageHandler.setStream("foo");

		assertThatExceptionOfType(MessageHandlingException.class)
				.isThrownBy(() -> this.kinesisSendChannel.send(message))
				.withCauseInstanceOf(IllegalStateException.class)
				.withStackTraceContaining("'partitionKey' must not be null for sending a Kinesis record");

		Message<?> message2 =
				MessageBuilder.fromMessage(message)
						.setHeader(AwsHeaders.PARTITION_KEY, "fooKey")
						.setHeader(AwsHeaders.SEQUENCE_NUMBER, "10")
						.build();

		this.kinesisSendChannel.send(message2);

		Message<?> success = this.successChannel.receive(10000);
		assertThat(success.getHeaders().get(AwsHeaders.PARTITION_KEY)).isEqualTo("fooKey");
		assertThat(success.getHeaders().get(AwsHeaders.SEQUENCE_NUMBER)).isEqualTo("10");
		assertThat(success.getPayload()).isEqualTo("message");

		message2 =
				MessageBuilder.fromMessage(message)
						.setHeader(AwsHeaders.PARTITION_KEY, "fooKey")
						.setHeader(AwsHeaders.SEQUENCE_NUMBER, "10")
						.build();

		this.kinesisSendChannel.send(message2);

		Message<?> failed = this.errorChannel.receive(10000);
		AwsRequestFailureException putRecordFailure = (AwsRequestFailureException) failed.getPayload();
		assertThat(putRecordFailure.getCause().getMessage()).isEqualTo("putRecordRequestEx");
		assertThat(((PutRecordRequest) putRecordFailure.getRequest()).streamName()).isEqualTo("foo");
		assertThat(((PutRecordRequest) putRecordFailure.getRequest()).partitionKey()).isEqualTo("fooKey");
		assertThat(((PutRecordRequest) putRecordFailure.getRequest()).sequenceNumberForOrdering()).isEqualTo("10");
		assertThat(((PutRecordRequest) putRecordFailure.getRequest()).explicitHashKey()).isNull();
		assertThat(((PutRecordRequest) putRecordFailure.getRequest()).data())
				.isEqualTo(SdkBytes.fromUtf8String("message"));

		PutRecordsRequestEntry testRecordEntry =
				PutRecordsRequestEntry.builder()
						.data(SdkBytes.fromUtf8String("test"))
						.partitionKey("testKey")
						.build();

		message2 =
				MessageBuilder.withPayload(
								PutRecordsRequest.builder()
										.streamName("myStream")
										.records(testRecordEntry)
										.build())
						.setErrorChannel(this.errorChannel)
						.build();

		this.kinesisSendChannel.send(message2);

		success = this.successChannel.receive(10000);
		assertThat(((PutRecordsRequest) success.getPayload()).records())
				.containsExactlyInAnyOrder(testRecordEntry);

		this.kinesisSendChannel.send(message2);

		failed = this.errorChannel.receive(10000);
		AwsRequestFailureException putRecordsFailure = (AwsRequestFailureException) failed.getPayload();
		assertThat(putRecordsFailure.getCause().getMessage()).isEqualTo("putRecordsRequestEx");
		assertThat(((PutRecordsRequest) putRecordsFailure.getRequest()).streamName()).isEqualTo("myStream");
		assertThat(((PutRecordsRequest) putRecordsFailure.getRequest()).records())
				.containsExactlyInAnyOrder(testRecordEntry);
	}

	@Configuration
	@EnableIntegration
	public static class ContextConfiguration {

		@Bean
		public KinesisAsyncClient amazonKinesis() {
			KinesisAsyncClient mock = mock(KinesisAsyncClient.class);

			given(mock.putRecord(any(PutRecordRequest.class)))
					.willAnswer(invocation -> {
						PutRecordRequest request = invocation.getArgument(0);
						PutRecordResponse.Builder result =
								PutRecordResponse.builder()
										.sequenceNumber(request.sequenceNumberForOrdering())
										.shardId("shardId-1");
						return CompletableFuture.completedFuture(result.build());
					})
					.willAnswer(invocation ->
							CompletableFuture.failedFuture(new RuntimeException("putRecordRequestEx")));

			given(mock.putRecords(any(PutRecordsRequest.class)))
					.willAnswer(invocation -> CompletableFuture.completedFuture(PutRecordsResponse.builder().build()))
					.willAnswer(invocation ->
							CompletableFuture.failedFuture(new RuntimeException("putRecordsRequestEx")));

			return mock;
		}

		@Bean
		public PollableChannel errorChannel() {
			return new QueueChannel();
		}

		@Bean
		public PollableChannel successChannel() {
			return new QueueChannel();
		}

		@Bean
		@ServiceActivator(inputChannel = "kinesisSendChannel")
		public MessageHandler kinesisMessageHandler() {
			KinesisMessageHandler kinesisMessageHandler = new KinesisMessageHandler(amazonKinesis());
			kinesisMessageHandler.setAsync(true);
			kinesisMessageHandler.setOutputChannel(successChannel());
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
			return kinesisMessageHandler;
		}

	}

}
