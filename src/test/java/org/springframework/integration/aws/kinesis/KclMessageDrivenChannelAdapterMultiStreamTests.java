/*
 * Copyright 2023 the original author or authors.
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

package org.springframework.integration.aws.kinesis;

import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.Consumer;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamConsumersRequest;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.aws.LocalstackContainerTest;
import org.springframework.integration.aws.inbound.kinesis.KclMessageDrivenChannelAdapter;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.messaging.Message;
import org.springframework.messaging.PollableChannel;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Siddharth Jain
 *
 * @since 3.0
 */
@SpringJUnitConfig
@DirtiesContext

public class KclMessageDrivenChannelAdapterMultiStreamTests implements LocalstackContainerTest {

	private static final String TEST_STREAM1 = "MultiStreamKcl1";

	private static final String TEST_STREAM2 = "MultiStreamKcl2";

	private static KinesisAsyncClient AMAZON_KINESIS;

	private static DynamoDbAsyncClient DYNAMO_DB;

	private static CloudWatchAsyncClient CLOUD_WATCH;

	@Autowired
	private KclMessageDrivenChannelAdapter adapter;

	@Autowired
	private PollableChannel kinesisReceiveChannel;

	@BeforeAll
	static void setup() {
		AMAZON_KINESIS = LocalstackContainerTest.kinesisClient();
		DYNAMO_DB = LocalstackContainerTest.dynamoDbClient();
		CLOUD_WATCH = LocalstackContainerTest.cloudWatchClient();

		AMAZON_KINESIS.createStream(request -> request.streamName(TEST_STREAM1).shardCount(1))
				.thenCompose(result -> AMAZON_KINESIS.waiter()
						.waitUntilStreamExists(request -> request.streamName(TEST_STREAM1)))
				.join();

		AMAZON_KINESIS.createStream(request -> request.streamName(TEST_STREAM2).shardCount(1))
				.thenCompose(result -> AMAZON_KINESIS.waiter().
						waitUntilStreamExists(request -> request.streamName(TEST_STREAM2)))
				.join();
	}

	@AfterAll
	static void tearDown() {
		AMAZON_KINESIS.deleteStream(request -> request.streamName(TEST_STREAM1).enforceConsumerDeletion(true))
				.thenCompose(result -> AMAZON_KINESIS.waiter()
						.waitUntilStreamNotExists(request -> request.streamName(TEST_STREAM1)))
				.join();

		AMAZON_KINESIS.deleteStream(request -> request.streamName(TEST_STREAM2).enforceConsumerDeletion(true))
				.thenCompose(result -> AMAZON_KINESIS.waiter()
						.waitUntilStreamNotExists(request -> request.streamName(TEST_STREAM2)))
				.join();
	}

	@Test
	@Disabled
	void kclChannelAdapterReceivesRecords() {
		String testData = "test data";
		AMAZON_KINESIS.putRecord(request -> request
				.streamName(TEST_STREAM1)
				.data(SdkBytes.fromUtf8String(testData))
				.partitionKey("test"));
		// We need so long delay because KCL has a more than a minute setup phase.
		Message<?> receive = this.kinesisReceiveChannel.receive(300_000);
		assertThat(receive).isNotNull();
		assertThat(receive.getPayload()).isEqualTo(testData);
		assertThat(receive.getHeaders()).containsKey(IntegrationMessageHeaderAccessor.SOURCE_DATA);
		assertThat(receive.getHeaders().get(AwsHeaders.RECEIVED_SEQUENCE_NUMBER, String.class)).isNotEmpty();
	}

	@Test
	public void kclChannelAdapter_shouldRegisterStreamConsumer() {
		String testData = "test data";
		AMAZON_KINESIS.putRecord(request -> request
				.streamName(TEST_STREAM1)
				.data(SdkBytes.fromUtf8String(testData))
				.partitionKey("test"));
		// The below statement works but with a higher timeout. For 2 streams, this takes too long.
		Message<?> receive = this.kinesisReceiveChannel.receive(300_000);
		DescribeStreamResponse streamSummary = AMAZON_KINESIS.describeStream(
					request -> 	request.streamName(TEST_STREAM1)).join();
		List<Consumer> stream1Consumers = AMAZON_KINESIS.listStreamConsumers(
					request -> request.streamARN(streamSummary.streamDescription().streamARN())).join().consumers();

		DescribeStreamResponse stream2Summary = AMAZON_KINESIS.describeStream(
				request -> request.streamName(TEST_STREAM2)).join();
		List<Consumer> stream2Consumers = AMAZON_KINESIS.listStreamConsumers(
				request -> request.streamARN(stream2Summary.streamDescription().streamARN())).join().consumers();

		assertThat(stream1Consumers.size()).isEqualTo(1);
		assertThat(stream2Consumers.size()).isEqualTo(1);
	}

	@Configuration
	@EnableIntegration
	public static class TestConfiguration {
		@Bean
		public KclMessageDrivenChannelAdapter kclMessageDrivenChannelAdapter() {
			String[] multiInputStreams = new String[] {TEST_STREAM1, TEST_STREAM2};
			KclMessageDrivenChannelAdapter adapter = new KclMessageDrivenChannelAdapter(AMAZON_KINESIS, CLOUD_WATCH, DYNAMO_DB, multiInputStreams);
			adapter.setOutputChannel(kinesisReceiveChannel());
			adapter.setStreamInitialSequence(InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON));
			adapter.setConverter(String::new);
			adapter.setBindSourceRecord(true);
			return adapter;
		}

		// Need to find out if for 2 way communication in EFO Consumer, a Pollable-Channel is the correct channel?
		@Bean
		public PollableChannel kinesisReceiveChannel() {
			return new QueueChannel();
		}
	}
}
