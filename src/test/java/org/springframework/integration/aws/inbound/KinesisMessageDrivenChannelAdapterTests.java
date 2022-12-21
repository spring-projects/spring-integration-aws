/*
 * Copyright 2017-2022 the original author or authors.
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

package org.springframework.integration.aws.inbound;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.serializer.support.SerializingConverter;
import org.springframework.integration.aws.event.KinesisShardEndedEvent;
import org.springframework.integration.aws.inbound.kinesis.CheckpointMode;
import org.springframework.integration.aws.inbound.kinesis.Checkpointer;
import org.springframework.integration.aws.inbound.kinesis.KinesisMessageDrivenChannelAdapter;
import org.springframework.integration.aws.inbound.kinesis.KinesisShardOffset;
import org.springframework.integration.aws.inbound.kinesis.ListenerMode;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.metadata.MetadataStore;
import org.springframework.integration.metadata.SimpleMetadataStore;
import org.springframework.integration.support.locks.DefaultLockRegistry;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.PollableChannel;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Artem Bilan
 * @author Matthias Wesolowski
 * @author Greg Eales
 * @author Asiel Caballero
 * @author Jonathan Nagayoshi
 *
 * @since 1.1
 */
@SpringJUnitConfig
@DirtiesContext
public class KinesisMessageDrivenChannelAdapterTests {

	private static final String STREAM1 = "stream1";

	private static final String STREAM_FOR_RESHARDING = "streamForResharding";

	@Autowired
	private QueueChannel kinesisChannel;

	@Autowired
	@Qualifier("kinesisMessageDrivenChannelAdapter")
	private KinesisMessageDrivenChannelAdapter kinesisMessageDrivenChannelAdapter;

	@Autowired
	private MetadataStore checkpointStore;

	@Autowired
	private MetadataStore reshardingCheckpointStore;

	@Autowired
	@Qualifier("reshardingChannelAdapter")
	private KinesisMessageDrivenChannelAdapter reshardingChannelAdapter;

	@Autowired
	private AmazonKinesis amazonKinesisForResharding;

	@Autowired
	private Config config;

	@BeforeEach
	void setup() {
		this.kinesisChannel.purge(null);
	}

	@Test
	@SuppressWarnings({ "unchecked", "rawtypes" })
	void testKinesisMessageDrivenChannelAdapter() {
		this.kinesisMessageDrivenChannelAdapter.start();
		final Set<KinesisShardOffset> shardOffsets = TestUtils.getPropertyValue(this.kinesisMessageDrivenChannelAdapter,
				"shardOffsets", Set.class);

		KinesisShardOffset testOffset1 = KinesisShardOffset.latest(STREAM1, "1");
		KinesisShardOffset testOffset2 = KinesisShardOffset.latest(STREAM1, "2");
		synchronized (shardOffsets) {
			assertThat(shardOffsets).contains(testOffset1, testOffset2);
			assertThat(shardOffsets).doesNotContain(KinesisShardOffset.latest(STREAM1, "3"));
		}

		Map<KinesisShardOffset, ?> shardConsumers = TestUtils.getPropertyValue(this.kinesisMessageDrivenChannelAdapter,
				"shardConsumers", Map.class);

		await().untilAsserted(() -> assertThat(shardConsumers.keySet()).contains(testOffset1, testOffset2));

		assertThat(shardConsumers).containsKeys(testOffset1, testOffset2);

		Message<?> message = this.kinesisChannel.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isEqualTo("foo");
		MessageHeaders headers = message.getHeaders();
		assertThat(headers.get(AwsHeaders.RECEIVED_PARTITION_KEY)).isEqualTo("partition1");
		assertThat(headers.get(AwsHeaders.SHARD)).isEqualTo("1");
		assertThat(headers.get(AwsHeaders.RECEIVED_SEQUENCE_NUMBER)).isEqualTo("1");
		assertThat(headers.get(AwsHeaders.RECEIVED_STREAM)).isEqualTo(STREAM1);
		Checkpointer checkpointer = headers.get(AwsHeaders.CHECKPOINTER, Checkpointer.class);
		assertThat(checkpointer).isNotNull();

		checkpointer.checkpoint();

		message = this.kinesisChannel.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isEqualTo("bar");
		headers = message.getHeaders();
		assertThat(headers.get(AwsHeaders.RECEIVED_PARTITION_KEY)).isEqualTo("partition1");
		assertThat(headers.get(AwsHeaders.SHARD)).isEqualTo("1");
		assertThat(headers.get(AwsHeaders.RECEIVED_SEQUENCE_NUMBER)).isEqualTo("2");
		assertThat(headers.get(AwsHeaders.RECEIVED_STREAM)).isEqualTo(STREAM1);

		assertThat(this.kinesisChannel.receive(10)).isNull();

		assertThat(this.checkpointStore.get("SpringIntegration" + ":" + STREAM1 + ":" + "1")).isEqualTo("2");

		this.kinesisMessageDrivenChannelAdapter.stop();

		Map<?, ?> forLocking = TestUtils.getPropertyValue(this.kinesisMessageDrivenChannelAdapter,
				"shardConsumerManager.locks", Map.class);

		await().untilAsserted(() -> assertThat(forLocking).hasSize(0));

		final List consumerInvokers = TestUtils.getPropertyValue(this.kinesisMessageDrivenChannelAdapter,
				"consumerInvokers", List.class);
		await().untilAsserted(() -> assertThat(consumerInvokers).hasSize(0));

		this.kinesisMessageDrivenChannelAdapter.setListenerMode(ListenerMode.batch);
		this.kinesisMessageDrivenChannelAdapter.setCheckpointMode(CheckpointMode.record);
		this.checkpointStore.put("SpringIntegration" + ":" + STREAM1 + ":" + "1", "1");

		setup();

		this.kinesisMessageDrivenChannelAdapter.start();

		message = this.kinesisChannel.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isInstanceOf(List.class);
		List<String> payload = (List<String>) message.getPayload();
		assertThat(payload).size().isEqualTo(1);
		String record = payload.get(0);
		assertThat(record).isEqualTo("bar");

		Object partitionKeyHeader = message.getHeaders().get(AwsHeaders.RECEIVED_PARTITION_KEY);
		assertThat(partitionKeyHeader).isInstanceOf(List.class);
		assertThat((List<String>) partitionKeyHeader).contains("partition1");

		Object sequenceNumberHeader = message.getHeaders().get(AwsHeaders.RECEIVED_SEQUENCE_NUMBER);
		assertThat(sequenceNumberHeader).isInstanceOf(List.class);
		assertThat((List<String>) sequenceNumberHeader).contains("2");

		await().untilAsserted(
				() -> assertThat(this.checkpointStore.get("SpringIntegration" + ":" + STREAM1 + ":" + "1"))
						.isEqualTo("2"));

		assertThat(TestUtils.getPropertyValue(this.kinesisMessageDrivenChannelAdapter, "consumerInvokers", List.class))
				.hasSize(2);

		this.kinesisMessageDrivenChannelAdapter.stop();

		this.kinesisMessageDrivenChannelAdapter.setListenerMode(ListenerMode.batch);
		this.kinesisMessageDrivenChannelAdapter.setCheckpointMode(CheckpointMode.manual);
		this.checkpointStore.put("SpringIntegration" + ":" + STREAM1 + ":" + "1", "2");

		setup();

		this.kinesisMessageDrivenChannelAdapter.start();

		message = this.kinesisChannel.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isInstanceOf(List.class);
		List<String> messagePayload = (List<String>) message.getPayload();
		assertThat(messagePayload).size().isEqualTo(3);

		Object messageSequenceNumberHeader = message.getHeaders().get(AwsHeaders.RECEIVED_SEQUENCE_NUMBER);
		assertThat(messageSequenceNumberHeader).isInstanceOf(List.class);
		assertThat((List<String>) messageSequenceNumberHeader).contains("3");
		// Set checkpoint to 3, this should prevent adapter from using next shard, since its not the latest record
		// in the batch
		checkpointer.checkpoint("3");

		await().untilAsserted(
				() -> assertThat(this.checkpointStore.get("SpringIntegration" + ":" + STREAM1 + ":" + "1"))
						.isEqualTo("3"));
		message = this.kinesisChannel.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isInstanceOf(List.class);
		messagePayload = (List<String>) message.getPayload();
		assertThat(messagePayload).size().isEqualTo(2);
		assertThat(messagePayload).contains("bar");
		assertThat(messagePayload).contains("foobar");

		this.kinesisMessageDrivenChannelAdapter.stop();


	}

	@Test
	@SuppressWarnings("rawtypes")
	void testResharding() throws InterruptedException {
		this.reshardingChannelAdapter.start();

		assertThat(this.kinesisChannel.receive(10000)).isNotNull();

		Map shardConsumers = TestUtils.getPropertyValue(this.reshardingChannelAdapter, "shardConsumers", Map.class);

		int n = 0;
		while (shardConsumers.size() != 4 && n++ < 100) {
			Thread.sleep(100);
		}
		assertThat(n).isLessThan(100);


		// When resharding happens the describeStream() is performed again
		verify(this.amazonKinesisForResharding, atLeast(1))
				.listShards(any(ListShardsRequest.class));

		this.reshardingChannelAdapter.stop();

		assertThat(this.reshardingCheckpointStore.get("SpringIntegration:streamForResharding:closedEmptyShard5"))
				.isEqualTo("50");

		KinesisShardEndedEvent kinesisShardEndedEvent = this.config.shardEndedEventReference.get();

		assertThat(kinesisShardEndedEvent).isNotNull()
				.extracting(KinesisShardEndedEvent::getShardKey)
				.isEqualTo("SpringIntegration:streamForResharding:closedEmptyShard5");
	}

	@Configuration
	@EnableIntegration
	public static class Config {
		private final AtomicReference<KinesisShardEndedEvent> shardEndedEventReference = new AtomicReference<>();

		@Bean
		public AmazonKinesis amazonKinesis() {
			AmazonKinesis amazonKinesis = mock(AmazonKinesis.class);

			given(amazonKinesis.listShards(new ListShardsRequest().withStreamName(STREAM1))).willReturn(
					new ListShardsResult()
							.withShards(new Shard().withShardId("1").withSequenceNumberRange(new SequenceNumberRange()),
									new Shard().withShardId("2").withSequenceNumberRange(new SequenceNumberRange()),
									new Shard().withShardId("3").withSequenceNumberRange(
											new SequenceNumberRange().withEndingSequenceNumber("1")))
			);

			String shard1Iterator1 = "shard1Iterator1";
			String shard1Iterator2 = "shard1Iterator2";

			given(amazonKinesis.getShardIterator(KinesisShardOffset.latest(STREAM1, "1").toShardIteratorRequest()))
					.willReturn(new GetShardIteratorResult().withShardIterator(shard1Iterator1),
							new GetShardIteratorResult().withShardIterator(shard1Iterator2));

			String shard2Iterator1 = "shard2Iterator1";

			given(amazonKinesis.getShardIterator(KinesisShardOffset.latest(STREAM1, "2").toShardIteratorRequest()))
					.willReturn(new GetShardIteratorResult().withShardIterator(shard2Iterator1));

			given(amazonKinesis.getRecords(new GetRecordsRequest().withShardIterator(shard1Iterator1).withLimit(25)))
					.willThrow(new ProvisionedThroughputExceededException("Iterator throttled"))
					.willThrow(new ExpiredIteratorException("Iterator expired"));

			SerializingConverter serializingConverter = new SerializingConverter();

			String shard1Iterator3 = "shard1Iterator3";

			given(amazonKinesis.getRecords(new GetRecordsRequest().withShardIterator(shard1Iterator2).withLimit(25)))
					.willReturn(new GetRecordsResult().withNextShardIterator(shard1Iterator3).withRecords(
							new Record().withPartitionKey("partition1").withSequenceNumber("1")
									.withData(ByteBuffer.wrap(serializingConverter.convert("foo"))),
							new Record().withPartitionKey("partition1").withSequenceNumber("2")
									.withData(ByteBuffer.wrap(serializingConverter.convert("bar")))));

			given(amazonKinesis.getRecords(new GetRecordsRequest().withShardIterator(shard2Iterator1).withLimit(25)))
					.willReturn(new GetRecordsResult().withNextShardIterator(shard2Iterator1));

			given(amazonKinesis.getRecords(new GetRecordsRequest().withShardIterator(shard1Iterator3).withLimit(25)))
					.willReturn(new GetRecordsResult().withNextShardIterator(shard1Iterator3));

			String shard1Iterator4 = "shard1Iterator4";

			given(amazonKinesis.getShardIterator(
					KinesisShardOffset.afterSequenceNumber(STREAM1, "1", "1").toShardIteratorRequest()))
					.willReturn(new GetShardIteratorResult().withShardIterator(shard1Iterator4));

			given(amazonKinesis.getRecords(new GetRecordsRequest().withShardIterator(shard1Iterator4).withLimit(25)))
					.willReturn(new GetRecordsResult().withNextShardIterator(shard1Iterator3)
							.withRecords(new Record().withPartitionKey("partition1").withSequenceNumber("2")
									.withData(ByteBuffer.wrap(serializingConverter.convert("bar")))));


			String shard1Iterator5 = "shard1Iterator5";
			String shard1Iterator6 = "shard1Iterator6";

			given(amazonKinesis.getShardIterator(
					KinesisShardOffset.afterSequenceNumber(STREAM1, "1", "2").toShardIteratorRequest()))
					.willReturn(new GetShardIteratorResult().withShardIterator(shard1Iterator5));

			given(amazonKinesis.getRecords(new GetRecordsRequest().withShardIterator(shard1Iterator5).withLimit(25)))
					.willReturn(new GetRecordsResult().withNextShardIterator(shard1Iterator6)
							.withRecords(new Record().withPartitionKey("partition1").withSequenceNumber("3")
									.withData(ByteBuffer.wrap(serializingConverter.convert("foo"))),
									new Record().withPartitionKey("partition1").withSequenceNumber("4")
											.withData(ByteBuffer.wrap(serializingConverter.convert("bar"))),
									new Record().withPartitionKey("partition1").withSequenceNumber("5")
											.withData(ByteBuffer.wrap(serializingConverter.convert("foobar")))));


			given(amazonKinesis.getShardIterator(
					KinesisShardOffset.afterSequenceNumber(STREAM1, "1", "3").toShardIteratorRequest()))
					.willReturn(new GetShardIteratorResult().withShardIterator(shard1Iterator6));

			given(amazonKinesis.getRecords(new GetRecordsRequest().withShardIterator(shard1Iterator6).withLimit(25)))
					.willReturn(new GetRecordsResult().withNextShardIterator(shard1Iterator6)
							.withRecords(
									new Record().withPartitionKey("partition1").withSequenceNumber("4")
											.withData(ByteBuffer.wrap(serializingConverter.convert("bar"))),
									new Record().withPartitionKey("partition1").withSequenceNumber("5")
											.withData(ByteBuffer.wrap(serializingConverter.convert("foobar")))));

			return amazonKinesis;
		}

		@Bean
		public ConcurrentMetadataStore checkpointStore() {
			SimpleMetadataStore simpleMetadataStore = new SimpleMetadataStore();
			String testKey = "SpringIntegration" + ":" + STREAM1 + ":" + "3";
			simpleMetadataStore.put(testKey, "1");
			return simpleMetadataStore;
		}

		@Bean
		public KinesisMessageDrivenChannelAdapter kinesisMessageDrivenChannelAdapter() {
			KinesisMessageDrivenChannelAdapter adapter = new KinesisMessageDrivenChannelAdapter(amazonKinesis(),
					STREAM1);
			adapter.setAutoStartup(false);
			adapter.setOutputChannel(kinesisChannel());
			adapter.setCheckpointStore(checkpointStore());
			adapter.setCheckpointMode(CheckpointMode.manual);
			adapter.setLockRegistry(new DefaultLockRegistry());
			adapter.setStartTimeout(10000);
			adapter.setDescribeStreamRetries(1);
			adapter.setConcurrency(10);
			adapter.setRecordsLimit(25);

			DirectFieldAccessor dfa = new DirectFieldAccessor(adapter);
			dfa.setPropertyValue("describeStreamBackoff", 10);
			dfa.setPropertyValue("consumerBackoff", 10);
			dfa.setPropertyValue("idleBetweenPolls", 1);

			return adapter;
		}

		@Bean
		public PollableChannel kinesisChannel() {
			return new QueueChannel();
		}

		@Bean
		public AmazonKinesis amazonKinesisForResharding() {
			AmazonKinesis amazonKinesis = mock(AmazonKinesis.class);

			// kinesis handles adding a shard by closing a shard and opening 2 new instead, creating a scenario where it
			// happens couple of times
			given(amazonKinesis.listShards(new ListShardsRequest().withStreamName(STREAM_FOR_RESHARDING)))
					.willReturn(new ListShardsResult()
							.withShards(
									new Shard().withShardId("closedShard1")
											.withSequenceNumberRange(new SequenceNumberRange()
													.withEndingSequenceNumber("10"))))
					.willReturn(new ListShardsResult()
							.withShards(
									new Shard().withShardId("closedShard1")
											.withSequenceNumberRange(new SequenceNumberRange()
													.withEndingSequenceNumber("10")),
									new Shard().withShardId("newShard2")
											.withSequenceNumberRange(new SequenceNumberRange()),
									new Shard().withShardId("newShard3")
											.withSequenceNumberRange(new SequenceNumberRange()),
									new Shard().withShardId("closedShard4")
											.withSequenceNumberRange(new SequenceNumberRange()
													.withEndingSequenceNumber("40")),
									new Shard().withShardId("closedEmptyShard5")
											.withSequenceNumberRange(new SequenceNumberRange()
													.withEndingSequenceNumber("50"))))
					.willReturn(new ListShardsResult()
							.withShards(
									new Shard().withShardId("closedShard1")
											.withSequenceNumberRange(new SequenceNumberRange()
													.withEndingSequenceNumber("10")),
									new Shard().withShardId("newShard2")
											.withSequenceNumberRange(new SequenceNumberRange()),
									new Shard().withShardId("newShard3")
											.withSequenceNumberRange(new SequenceNumberRange()),
									new Shard().withShardId("closedShard4")
											.withSequenceNumberRange(new SequenceNumberRange()
													.withEndingSequenceNumber("40")),
									new Shard().withShardId("closedEmptyShard5")
											.withSequenceNumberRange(new SequenceNumberRange()
													.withEndingSequenceNumber("50")),
									new Shard().withShardId("newShard6")
											.withSequenceNumberRange(new SequenceNumberRange()),
									new Shard().withShardId("newShard7")
											.withSequenceNumberRange(new SequenceNumberRange())));


			setClosedShard(amazonKinesis, "1");
			setNewShard(amazonKinesis, "2");
			setNewShard(amazonKinesis, "3");
			setClosedShard(amazonKinesis, "4");
			setClosedEmptyShard(amazonKinesis, "5");
			setNewShard(amazonKinesis, "6");
			setNewShard(amazonKinesis, "7");

			return amazonKinesis;
		}

		private void setClosedShard(AmazonKinesis amazonKinesis, String shardIndex) {
			String shardIterator = String.format("shard%sIterator1", shardIndex);

			given(amazonKinesis.getShardIterator(
					KinesisShardOffset.latest(STREAM_FOR_RESHARDING, "closedShard" + shardIndex)
							.toShardIteratorRequest()))
					.willReturn(new GetShardIteratorResult().withShardIterator(shardIterator));

			given(amazonKinesis.getRecords(new GetRecordsRequest().withShardIterator(shardIterator).withLimit(25)))
					.willReturn(new GetRecordsResult().withNextShardIterator(null)
							.withRecords(new Record().withPartitionKey("partition1").withSequenceNumber(shardIndex)
									.withData(ByteBuffer.wrap("foo".getBytes()))));
		}

		private void setClosedEmptyShard(AmazonKinesis amazonKinesis, String shardIndex) {
			String shardIterator = String.format("shard%sIterator1", shardIndex);

			given(amazonKinesis.getShardIterator(
					KinesisShardOffset.latest(STREAM_FOR_RESHARDING, "closedEmptyShard" + shardIndex)
							.toShardIteratorRequest()))
					.willReturn(new GetShardIteratorResult().withShardIterator(shardIterator));

			given(amazonKinesis.getRecords(new GetRecordsRequest().withShardIterator(shardIterator).withLimit(25)))
					.willReturn(new GetRecordsResult().withNextShardIterator(null));
		}

		private void setNewShard(AmazonKinesis amazonKinesis, String shardIndex) {
			String shardIterator1 = String.format("shard%sIterator1", shardIndex);
			String shardIterator2 = String.format("shard%sIterator2", shardIndex);

			given(amazonKinesis.getShardIterator(
					KinesisShardOffset.latest(STREAM_FOR_RESHARDING, "newShard" + shardIndex).toShardIteratorRequest()))
					.willReturn(new GetShardIteratorResult().withShardIterator(shardIterator1));

			given(amazonKinesis.getRecords(new GetRecordsRequest().withShardIterator(shardIterator2).withLimit(25)))
					.willReturn(new GetRecordsResult().withNextShardIterator(shardIterator2)
							.withRecords(new Record().withPartitionKey("partition1").withSequenceNumber(shardIndex)
									.withData(ByteBuffer.wrap("foo".getBytes()))));


			given(amazonKinesis.getShardIterator(
					KinesisShardOffset.latest(STREAM_FOR_RESHARDING, "newShard" + shardIndex).toShardIteratorRequest()))
					.willReturn(new GetShardIteratorResult().withShardIterator(shardIterator2));
		}

		@Bean
		public ConcurrentMetadataStore reshardingCheckpointStore() {
			return new ExceptionReadyMetadataStore();
		}

		@Bean
		public KinesisMessageDrivenChannelAdapter reshardingChannelAdapter() {
			KinesisMessageDrivenChannelAdapter adapter = new KinesisMessageDrivenChannelAdapter(
					amazonKinesisForResharding(), STREAM_FOR_RESHARDING);
			adapter.setAutoStartup(false);
			adapter.setOutputChannel(kinesisChannel());
			adapter.setStartTimeout(10000);
			adapter.setDescribeStreamRetries(1);
			adapter.setRecordsLimit(25);
			adapter.setConcurrency(1);
			adapter.setCheckpointStore(reshardingCheckpointStore());

			DirectFieldAccessor dfa = new DirectFieldAccessor(adapter);
			dfa.setPropertyValue("describeStreamBackoff", 10);
			dfa.setPropertyValue("consumerBackoff", 10);
			dfa.setPropertyValue("idleBetweenPolls", 1);

			adapter.setConverter(String::new);

			return adapter;
		}

		@EventListener
		public void handleKinesisShardEndedEvent(KinesisShardEndedEvent event) {
			this.shardEndedEventReference.set(event);
		}

	}

	private static class ExceptionReadyMetadataStore extends SimpleMetadataStore {
		@Override
		public boolean replace(String key, String oldValue, String newValue) {
			if ("SpringIntegration:streamForResharding:closedShard4".equals(key)) {
				throw new ProvisionedThroughputExceededException("Throughput exceeded");
			}

			return super.replace(key, oldValue, newValue);
		}
	}
}
