/*
 * Copyright 2023-2024 the original author or authors.
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
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.Consumer;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.NullMetricsFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.aws.LocalstackContainerTest;
import org.springframework.integration.aws.inbound.kinesis.KclMessageDrivenChannelAdapter;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.PollableChannel;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Artem Bilan
 * @author Siddharth Jain
 * @author Minkyu Moon
 *
 * @since 3.0
 */
@SpringJUnitConfig
@DirtiesContext
public class KclMessageDrivenChannelAdapterTests implements LocalstackContainerTest {

	private static final String TEST_STREAM = "TestStreamKcl";

	private static KinesisAsyncClient AMAZON_KINESIS;

	private static DynamoDbAsyncClient DYNAMO_DB;

	private static CloudWatchAsyncClient CLOUD_WATCH;

	@Autowired
	private PollableChannel kinesisReceiveChannel;

	@Autowired
	private KclMessageDrivenChannelAdapter kclMessageDrivenChannelAdapter;

	@BeforeAll
	static void setup() {
		AMAZON_KINESIS = LocalstackContainerTest.kinesisClient();
		DYNAMO_DB = LocalstackContainerTest.dynamoDbClient();
		CLOUD_WATCH = LocalstackContainerTest.cloudWatchClient();

		AMAZON_KINESIS.createStream(request -> request.streamName(TEST_STREAM).shardCount(1))
				.thenCompose(result ->
						AMAZON_KINESIS.waiter().waitUntilStreamExists(request -> request.streamName(TEST_STREAM)))
				.join();
	}

	@AfterAll
	static void tearDown() {
		AMAZON_KINESIS
				.deleteStream(request -> request.streamName(TEST_STREAM).enforceConsumerDeletion(true))
				.thenCompose(result -> AMAZON_KINESIS.waiter()
						.waitUntilStreamNotExists(request -> request.streamName(TEST_STREAM)))
				.join();
	}

	@Test
	void kclChannelAdapterReceivesRecords() {
		String testData = "test data";

		AMAZON_KINESIS.putRecord(request ->
				request.streamName(TEST_STREAM)
						.data(SdkBytes.fromUtf8String(testData))
						.partitionKey("test"));

		// We need so long delay because KCL has a more than a minute setup phase.
		Message<?> receive = this.kinesisReceiveChannel.receive(300_000);
		assertThat(receive).isNotNull();
		assertThat(receive.getPayload()).isEqualTo(testData);
		assertThat(receive.getHeaders()).containsKey(IntegrationMessageHeaderAccessor.SOURCE_DATA);
		assertThat(receive.getHeaders().get(AwsHeaders.RECEIVED_SEQUENCE_NUMBER, String.class)).isNotEmpty();

		List<Consumer> streamConsumers =
				AMAZON_KINESIS.describeStream(r -> r.streamName(TEST_STREAM))
						.thenCompose(describeStreamResponse ->
								AMAZON_KINESIS.listStreamConsumers(r ->
										r.streamARN(describeStreamResponse.streamDescription().streamARN())))
						.join()
						.consumers();

		// Because FanOut is false, there would be no Stream Consumers.
		assertThat(streamConsumers).hasSize(0);

		List<String> tableNames = DYNAMO_DB.listTables().join().tableNames();
		assertThat(tableNames).contains("test_table");
	}

	@Test
	public void metricsLevelOfMetricsConfigShouldBeSetToMetricsLevelOfAdapter() {
		MetricsLevel metricsLevel =
				TestUtils.getPropertyValue(this.kclMessageDrivenChannelAdapter,
						"scheduler.metricsConfig.metricsLevel",
						MetricsLevel.class);
		assertThat(metricsLevel).isEqualTo(MetricsLevel.NONE);
	}

	@Test
	public void metricsFactoryOfSchedulerShouldBeSetNullMetricsFactoryIfMetricsLevelIsNone() {
		MetricsFactory metricsFactory =
				TestUtils.getPropertyValue(this.kclMessageDrivenChannelAdapter,
						"scheduler.metricsFactory",
						MetricsFactory.class);
		assertThat(metricsFactory).isInstanceOf(NullMetricsFactory.class);
	}

	@Test
	public void maxLeasesForWorkerOverriddenByCustomizer() {
		Integer maxLeasesForWorker =
				TestUtils.getPropertyValue(this.kclMessageDrivenChannelAdapter,
						"scheduler.leaseCoordinator.leaseTaker.maxLeasesForWorker",
						Integer.class);
		assertThat(maxLeasesForWorker).isEqualTo(10);
	}

	@Test
	public void shardConsumerDispatchPollIntervalMillisOverriddenByCustomizer() {
		Long shardConsumerDispatchPollIntervalMillis =
				TestUtils.getPropertyValue(this.kclMessageDrivenChannelAdapter,
						"scheduler.shardConsumerDispatchPollIntervalMillis",
						Long.class);
		assertThat(shardConsumerDispatchPollIntervalMillis).isEqualTo(500L);
	}

	@Test
	public void pollingMaxRecordsIsPropagated() {
		Integer maxRecords =
				TestUtils.getPropertyValue(this.kclMessageDrivenChannelAdapter,
						"scheduler.retrievalConfig.retrievalSpecificConfig.maxRecords",
						Integer.class);
		assertThat(maxRecords).isEqualTo(99);
	}

	@Configuration
	@EnableIntegration
	public static class TestConfiguration {

		@Bean
		public KclMessageDrivenChannelAdapter kclMessageDrivenChannelAdapter() {
			KclMessageDrivenChannelAdapter adapter =
					new KclMessageDrivenChannelAdapter(AMAZON_KINESIS, CLOUD_WATCH, DYNAMO_DB, TEST_STREAM);
			adapter.setOutputChannel(kinesisReceiveChannel());
			adapter.setStreamInitialSequence(
					InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON));
			adapter.setConverter(String::new);
			adapter.setConsumerGroup("single_stream_group");
			adapter.setLeaseTableName("test_table");
			adapter.setFanOut(false);
			adapter.setMetricsLevel(MetricsLevel.NONE);
			adapter.setLeaseManagementConfigCustomizer(leaseManagementConfig ->
					leaseManagementConfig.maxLeasesForWorker(10));
			adapter.setCoordinatorConfigCustomizer(coordinatorConfig ->
					coordinatorConfig.shardConsumerDispatchPollIntervalMillis(500L));
			adapter.setBindSourceRecord(true);
			adapter.setEmptyRecordList(true);
			adapter.setPollingMaxRecords(99);
			return adapter;
		}

		@Bean
		public PollableChannel kinesisReceiveChannel() {
			return new QueueChannel();
		}

	}

}
