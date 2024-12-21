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

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecord;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Siddharth Jain
 * @since 3.0.9
 */
@SpringJUnitConfig
@DirtiesContext
public class KplMessageHandlerTests {

	@Autowired
	protected KinesisProducer kinesisProducer;

	@Autowired
	protected MessageChannel kinesisSendChannel;

	@Autowired
	protected KplMessageHandler kplMessageHandler;

	@Test
	@SuppressWarnings("unchecked")
	void testKPLMessageHandler_raw_payload_success() {
		given(this.kinesisProducer.addUserRecord(any(UserRecord.class)))
				.willReturn(mock(ListenableFuture.class));
		final Message<?> message = MessageBuilder
				.withPayload("message1")
				.setHeader(AwsHeaders.PARTITION_KEY, "fooKey")
				.setHeader(AwsHeaders.SEQUENCE_NUMBER, "10")
				.setHeader("foo", "bar")
				.build();


		ArgumentCaptor<UserRecord> userRecordRequestArgumentCaptor = ArgumentCaptor
				.forClass(UserRecord.class);

		this.kinesisSendChannel.send(message);
		verify(this.kinesisProducer).addUserRecord(userRecordRequestArgumentCaptor.capture());

		UserRecord userRecord = userRecordRequestArgumentCaptor.getValue();
		assertThat(userRecord.getStreamName()).isEqualTo("foo");
		assertThat(userRecord.getPartitionKey()).isEqualTo("fooKey");
		assertThat(userRecord.getExplicitHashKey()).isNull();
	}

	@Test
	@SuppressWarnings("unchecked")
	void testKPLMessageHandler_raw_payload_backpressure_capacity_available_Test() {
		given(this.kinesisProducer.addUserRecord(any(UserRecord.class)))
				.willReturn(mock(ListenableFuture.class));
		this.kplMessageHandler.setMaxInFlightRecords(2);
		this.kplMessageHandler.setMaxInFlightRecordsInitBackoffDuration(100);
		this.kplMessageHandler.setMaxInFlightRecordsBackoffMaxAttempts(2);
		this.kplMessageHandler.setMaxInFlightRecordsBackoffRate(2);
		given(this.kinesisProducer.getOutstandingRecordsCount())
				.willReturn(1);
		final Message<?> message = MessageBuilder
				.withPayload("message1")
				.setHeader(AwsHeaders.PARTITION_KEY, "fooKey")
				.setHeader(AwsHeaders.SEQUENCE_NUMBER, "10")
				.setHeader("foo", "bar")
				.build();


		ArgumentCaptor<UserRecord> userRecordRequestArgumentCaptor = ArgumentCaptor
				.forClass(UserRecord.class);

		this.kinesisSendChannel.send(message);
		verify(this.kinesisProducer).addUserRecord(userRecordRequestArgumentCaptor.capture());
		verify(this.kinesisProducer, Mockito.times(1)).getOutstandingRecordsCount();
		UserRecord userRecord = userRecordRequestArgumentCaptor.getValue();
		assertThat(userRecord.getStreamName()).isEqualTo("foo");
		assertThat(userRecord.getPartitionKey()).isEqualTo("fooKey");
		assertThat(userRecord.getExplicitHashKey()).isNull();
	}

	@Test
	@SuppressWarnings("unchecked")
	void testKPLMessageHandler_raw_payload_backpressure_insuff_capacity_test() {
		given(this.kinesisProducer.addUserRecord(any(UserRecord.class)))
				.willReturn(mock(ListenableFuture.class));
		this.kplMessageHandler.setMaxInFlightRecords(2);
		this.kplMessageHandler.setMaxInFlightRecordsInitBackoffDuration(100);
		this.kplMessageHandler.setMaxInFlightRecordsBackoffMaxAttempts(2);
		this.kplMessageHandler.setMaxInFlightRecordsBackoffRate(2);
		given(this.kinesisProducer.getOutstandingRecordsCount())
				.willReturn(3)
				.willReturn(2)
				.willReturn(1)
				.willReturn(0);
		final Message<?> message = MessageBuilder
				.withPayload("message1")
				.setHeader(AwsHeaders.PARTITION_KEY, "fooKey")
				.setHeader(AwsHeaders.SEQUENCE_NUMBER, "10")
				.setHeader("foo", "bar")
				.build();


		ArgumentCaptor<UserRecord> userRecordRequestArgumentCaptor = ArgumentCaptor
				.forClass(UserRecord.class);

		this.kinesisSendChannel.send(message);
		verify(this.kinesisProducer).addUserRecord(userRecordRequestArgumentCaptor.capture());
		verify(this.kinesisProducer, Mockito.times(3)).getOutstandingRecordsCount();
		UserRecord userRecord = userRecordRequestArgumentCaptor.getValue();
		assertThat(userRecord.getStreamName()).isEqualTo("foo");
		assertThat(userRecord.getPartitionKey()).isEqualTo("fooKey");
		assertThat(userRecord.getExplicitHashKey()).isNull();
	}

	@Test
	@SuppressWarnings("unchecked")
	void testKPLMessageHandler_raw_payload_backpressure_failure_test() {
		given(this.kinesisProducer.addUserRecord(any(UserRecord.class)))
				.willReturn(mock(ListenableFuture.class));
		this.kplMessageHandler.setMaxInFlightRecords(2);
		this.kplMessageHandler.setMaxInFlightRecordsInitBackoffDuration(100);
		this.kplMessageHandler.setMaxInFlightRecordsBackoffMaxAttempts(2);
		this.kplMessageHandler.setMaxInFlightRecordsBackoffRate(2);
		given(this.kinesisProducer.getOutstandingRecordsCount())
				.willReturn(5)
				.willReturn(4)
				.willReturn(3);
		final Message<?> message = MessageBuilder
				.withPayload("message1")
				.setHeader(AwsHeaders.PARTITION_KEY, "fooKey")
				.setHeader(AwsHeaders.SEQUENCE_NUMBER, "10")
				.setHeader("foo", "bar")
				.build();

		ArgumentCaptor<UserRecord> userRecordRequestArgumentCaptor = ArgumentCaptor
				.forClass(UserRecord.class);

		try {
			this.kinesisSendChannel.send(message);
		}
		catch (Exception ex) {
			assertThat(ex).isNotNull();
			assertThat(ex.getCause()).isNotNull();
			assertThat(ex.getCause().getClass()).isEqualTo(RuntimeException.class);
			assertThat(ex.getCause().getMessage()).isEqualTo("KPL Buffer already at max capacity.");
		}

		verify(this.kinesisProducer, Mockito.times(0)).addUserRecord(any(UserRecord.class));
		verify(this.kinesisProducer, Mockito.times(3)).getOutstandingRecordsCount();
	}

	@AfterEach
	public void tearDown() {
		clearInvocations(this.kinesisProducer);
	}

	@Configuration
	@EnableIntegration
	public static class ContextConfiguration {

		@Bean
		public KinesisProducer kinesisProducer() {
			return mock(KinesisProducer.class);
		}

		@Bean
		@ServiceActivator(inputChannel = "kinesisSendChannel")
		public MessageHandler kplMessageHandler(KinesisProducer kinesisProducer) {
			KplMessageHandler kplMessageHandler = new KplMessageHandler(kinesisProducer);
			kplMessageHandler.setAsync(true);
			kplMessageHandler.setStream("foo");
			return kplMessageHandler;
		}
	}

}
