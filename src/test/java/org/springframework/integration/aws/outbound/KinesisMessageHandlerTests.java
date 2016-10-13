/*
 * Copyright 2016 the original author or authors.
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
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.serializer.support.SerializingConverter;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;

/**
 * @author Artem Bilan
 * @since 1.1
 */
@RunWith(SpringRunner.class)
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

		ArgumentCaptor<PutRecordRequest> putRecordRequestArgumentCaptor =
				ArgumentCaptor.forClass(PutRecordRequest.class);
		verify(this.amazonKinesis).putRecordAsync(putRecordRequestArgumentCaptor.capture(),
				eq((AsyncHandler<PutRecordRequest, PutRecordResult>) this.asyncHandler));

		PutRecordRequest putRecordRequest = putRecordRequestArgumentCaptor.getValue();

		assertThat(putRecordRequest.getStreamName()).isEqualTo("foo");
		assertThat(putRecordRequest.getPartitionKey()).isEqualTo("fooKey");
		assertThat(putRecordRequest.getSequenceNumberForOrdering()).isEqualTo("10");
		assertThat(putRecordRequest.getExplicitHashKey()).isNull();
		assertThat(putRecordRequest.getData()).isEqualTo(ByteBuffer.wrap("message".getBytes()));

		message = new GenericMessage<>(new PutRecordsRequest()
				.withStreamName("myStream")
				.withRecords(new PutRecordsRequestEntry()
						.withData(ByteBuffer.wrap("test".getBytes()))
						.withPartitionKey("testKey")));

		this.kinesisSendChannel.send(message);

		ArgumentCaptor<PutRecordsRequest> putRecordsRequestArgumentCaptor =
				ArgumentCaptor.forClass(PutRecordsRequest.class);
		verify(this.amazonKinesis).putRecordsAsync(putRecordsRequestArgumentCaptor.capture(),
				eq((AsyncHandler<PutRecordsRequest, PutRecordsResult>) this.asyncHandler));

		PutRecordsRequest putRecordsRequest = putRecordsRequestArgumentCaptor.getValue();

		assertThat(putRecordsRequest.getStreamName()).isEqualTo("myStream");
		assertThat(putRecordsRequest.getRecords())
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
