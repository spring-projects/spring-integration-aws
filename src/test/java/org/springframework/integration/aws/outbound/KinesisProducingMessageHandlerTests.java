/*
 * Copyright 2017 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
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
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;

/**
 * @author Jacob Severson
 *
 * @since 1.1
 */
@RunWith(SpringRunner.class)
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
	@SuppressWarnings("unchecked")
	public void testKinesisMessageHandler() {
		Message<?> message = MessageBuilder.withPayload("message").build();
		try {
			this.kinesisSendChannel.send(message);
		}
		catch (Exception e) {
			assertThat(e).isInstanceOf(MessageHandlingException.class);
			assertThat(e.getCause()).isInstanceOf(IllegalStateException.class);
			assertThat(e.getMessage()).contains("'stream' must not be null for sending a Kinesis record");
		}

		this.kinesisMessageHandler.setStream("foo");
		try {
			this.kinesisSendChannel.send(message);
		}
		catch (Exception e) {
			assertThat(e).isInstanceOf(MessageHandlingException.class);
			assertThat(e.getCause()).isInstanceOf(IllegalStateException.class);
			assertThat(e.getMessage()).contains("'partitionKey' must not be null for sending a Kinesis record");
		}

		message = MessageBuilder.fromMessage(message)
				.setHeader(AwsHeaders.PARTITION_KEY, "fooKey")
				.setHeader(AwsHeaders.SEQUENCE_NUMBER, "10")
				.build();

		this.kinesisSendChannel.send(message);

		Message<?> success = this.successChannel.receive(10000);
		assertThat(success.getHeaders().get(AwsHeaders.PARTITION_KEY)).isEqualTo("fooKey");
		assertThat(success.getHeaders().get(AwsHeaders.SEQUENCE_NUMBER)).isEqualTo("10");
		assertThat(success.getPayload()).isEqualTo("message");

		message = MessageBuilder.fromMessage(message)
				.setHeader(AwsHeaders.PARTITION_KEY, "fooKey")
				.setHeader(AwsHeaders.SEQUENCE_NUMBER, "10")
				.build();

		this.kinesisSendChannel.send(message);

		Message<?> failed = this.errorChannel.receive(10000);
		AwsRequestFailureException putRecordFailure = (AwsRequestFailureException) failed.getPayload();
		assertThat(putRecordFailure.getCause().getMessage()).isEqualTo("putRecordRequestEx");
		assertThat(((PutRecordRequest) putRecordFailure.getRequest()).getStreamName()).isEqualTo("foo");
		assertThat(((PutRecordRequest) putRecordFailure.getRequest()).getPartitionKey()).isEqualTo("fooKey");
		assertThat(((PutRecordRequest) putRecordFailure.getRequest()).getSequenceNumberForOrdering()).isEqualTo("10");
		assertThat(((PutRecordRequest) putRecordFailure.getRequest()).getExplicitHashKey()).isNull();
		assertThat(((PutRecordRequest) putRecordFailure.getRequest())
				.getData()).isEqualTo(ByteBuffer.wrap("message".getBytes()));

		message = new GenericMessage<>(new PutRecordsRequest()
				.withStreamName("myStream")
				.withRecords(new PutRecordsRequestEntry()
						.withData(ByteBuffer.wrap("test".getBytes()))
						.withPartitionKey("testKey")));

		this.kinesisSendChannel.send(message);

		success = this.successChannel.receive(10000);
		assertThat(((PutRecordsRequest) success.getPayload()).getRecords())
				.containsExactlyInAnyOrder(new PutRecordsRequestEntry()
						.withData(ByteBuffer.wrap("test".getBytes()))
						.withPartitionKey("testKey"));

		message = new GenericMessage<>(new PutRecordsRequest()
				.withStreamName("myStream")
				.withRecords(new PutRecordsRequestEntry()
						.withData(ByteBuffer.wrap("test".getBytes()))
						.withPartitionKey("testKey")));

		this.kinesisSendChannel.send(message);

		failed = this.errorChannel.receive(10000);
		AwsRequestFailureException putRecordsFailure = (AwsRequestFailureException) failed.getPayload();
		assertThat(putRecordsFailure.getCause().getMessage()).isEqualTo("putRecordsRequestEx");
		assertThat(((PutRecordsRequest) putRecordsFailure.getRequest()).getStreamName()).isEqualTo("myStream");
		assertThat(((PutRecordsRequest) putRecordsFailure.getRequest()).getRecords())
				.containsExactlyInAnyOrder(new PutRecordsRequestEntry()
						.withData(ByteBuffer.wrap("test".getBytes()))
						.withPartitionKey("testKey"));
	}

	@Configuration
	@EnableIntegration
	public static class ContextConfiguration {

		@Bean
		@SuppressWarnings("unchecked")
		public AmazonKinesisAsync amazonKinesis() {
			AmazonKinesisAsync mock = mock(AmazonKinesisAsync.class);

			given(mock.putRecordAsync(any(PutRecordRequest.class), any(AsyncHandler.class)))
					.willAnswer(invocation -> {
						PutRecordRequest request = invocation.getArgument(0);
						AsyncHandler<PutRecordRequest, PutRecordResult> handler = invocation.getArgument(1);
						PutRecordResult result = new PutRecordResult()
								.withSequenceNumber(request.getSequenceNumberForOrdering())
								.withShardId("shardId-1");
						handler.onSuccess(new PutRecordRequest(), result);
						return mock(Future.class);
					})
					.willAnswer(invocation -> {
						AsyncHandler<?, ?> handler = invocation.getArgument(1);
						handler.onError(new RuntimeException("putRecordRequestEx"));
						return mock(Future.class);
					});


			given(mock.putRecordsAsync(any(PutRecordsRequest.class), any(AsyncHandler.class)))
					.willAnswer(invocation -> {
						AsyncHandler<PutRecordsRequest, PutRecordsResult> handler = invocation.getArgument(1);
						handler.onSuccess(new PutRecordsRequest(), new PutRecordsResult());
						return mock(Future.class);
					})
					.willAnswer(invocation -> {
						AsyncHandler<?, ?> handler = invocation.getArgument(1);
						handler.onError(new RuntimeException("putRecordsRequestEx"));
						return mock(Future.class);
					});

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
			kinesisMessageHandler.setSync(true);
			kinesisMessageHandler.setOutputChannel(successChannel());
			kinesisMessageHandler.setFailureChannel(errorChannel());
			kinesisMessageHandler.setConverter(new Converter<Object, byte[]>() {

				private SerializingConverter serializingConverter = new SerializingConverter();

				@Override
				public byte[] convert(Object source) {
					if (source instanceof String) {
						return ((String) source).getBytes();
					}
					else {
						return this.serializingConverter.convert(source);
					}
				}

			});
			return kinesisMessageHandler;
		}

	}

}
