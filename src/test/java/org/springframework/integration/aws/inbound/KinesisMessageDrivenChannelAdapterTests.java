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

package org.springframework.integration.aws.inbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.serializer.support.DeserializingConverter;
import org.springframework.core.serializer.support.SerializingConverter;
import org.springframework.integration.aws.inbound.kinesis.CheckpointMode;
import org.springframework.integration.aws.inbound.kinesis.Checkpointer;
import org.springframework.integration.aws.inbound.kinesis.KinesisMessageDrivenChannelAdapter;
import org.springframework.integration.aws.inbound.kinesis.KinesisShardOffset;
import org.springframework.integration.aws.inbound.kinesis.ListenerMode;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.metadata.MetadataStore;
import org.springframework.integration.metadata.SimpleMetadataStore;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.PollableChannel;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.kinesis.model.StreamStatus;

/**
 * @author Artem Bilan
 * @since 1.1
 */
@RunWith(SpringRunner.class)
@DirtiesContext
public class KinesisMessageDrivenChannelAdapterTests {

	private static final String STREAM1 = "stream1";

	@Autowired
	private PollableChannel kinesisChannel;

	@Autowired
	private KinesisMessageDrivenChannelAdapter kinesisMessageDrivenChannelAdapter;

	@Autowired
	private MetadataStore checkpointStore;

	@Test
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testKinesisMessageDrivenChannelAdapter() throws InterruptedException {
		final Set<KinesisShardOffset> shardOffsets =
				TestUtils.getPropertyValue(this.kinesisMessageDrivenChannelAdapter, "shardOffsets", Set.class);


		KinesisShardOffset testOffset1 = KinesisShardOffset.latest(STREAM1, "1");
		KinesisShardOffset testOffset2 = KinesisShardOffset.latest(STREAM1, "2");
		synchronized (shardOffsets) {
			assertThat(shardOffsets).contains(testOffset1, testOffset2);
			assertThat(shardOffsets).doesNotContain(KinesisShardOffset.latest(STREAM1, "3"));
		}

		Map shardConsumers =
				TestUtils.getPropertyValue(this.kinesisMessageDrivenChannelAdapter, "shardConsumers", Map.class);

		assertThat(shardConsumers).containsKeys(testOffset1, testOffset2);

		Message<?> message = this.kinesisChannel.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isEqualTo("foo");
		MessageHeaders headers = message.getHeaders();
		assertThat(headers.get(AwsHeaders.PARTITION_KEY)).isEqualTo("partition1");
		assertThat(headers.get(AwsHeaders.SHARD)).isEqualTo("1");
		assertThat(headers.get(AwsHeaders.SEQUENCE_NUMBER)).isEqualTo("1");
		assertThat(headers.get(AwsHeaders.STREAM)).isEqualTo(STREAM1);
		Checkpointer checkpointer = headers.get(AwsHeaders.CHECKPOINTER, Checkpointer.class);
		assertThat(checkpointer).isNotNull();

		checkpointer.checkpoint();

		message = this.kinesisChannel.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isEqualTo("bar");
		headers = message.getHeaders();
		assertThat(headers.get(AwsHeaders.PARTITION_KEY)).isEqualTo("partition1");
		assertThat(headers.get(AwsHeaders.SHARD)).isEqualTo("1");
		assertThat(headers.get(AwsHeaders.SEQUENCE_NUMBER)).isEqualTo("2");
		assertThat(headers.get(AwsHeaders.STREAM)).isEqualTo(STREAM1);

		assertThat(this.kinesisChannel.receive(10)).isNull();

		assertThat(this.checkpointStore.get("SpringIntegration" + ":" + STREAM1 + ":" + "1")).isEqualTo("2");

		this.kinesisMessageDrivenChannelAdapter.stop();
		this.kinesisMessageDrivenChannelAdapter.setListenerMode(ListenerMode.batch);
		this.kinesisMessageDrivenChannelAdapter
				.setCheckpointMode(CheckpointMode.record);
		this.checkpointStore.put("SpringIntegration" + ":" + STREAM1 + ":" + "1", "1");
		this.kinesisMessageDrivenChannelAdapter.start();

		message = this.kinesisChannel.receive(10000);
		assertThat(message).isNotNull();
		assertThat(message.getPayload()).isInstanceOf(List.class);
		List<Record> payload = (List<Record>) message.getPayload();
		assertThat(payload).size().isEqualTo(1);
		Record record = payload.get(0);
		assertThat(record.getPartitionKey()).isEqualTo("partition1");
		assertThat(record.getSequenceNumber()).isEqualTo("2");

		DeserializingConverter deserializingConverter = new DeserializingConverter();
		assertThat(deserializingConverter.convert(record.getData().array())).isEqualTo("bar");

		int n = 0;

		while (n++ < 100) {
			if (!this.checkpointStore.get("SpringIntegration" + ":" + STREAM1 + ":" + "1").equals("2")) {
				Thread.sleep(100);
			}
			else {
				break;
			}
		}

		assertThat(n).isLessThan(100);
		assertThat(this.checkpointStore.get("SpringIntegration" + ":" + STREAM1 + ":" + "1")).isEqualTo("2");

		List consumerInvoker =
				TestUtils.getPropertyValue(this.kinesisMessageDrivenChannelAdapter, "consumerInvokers", List.class);
		assertThat(consumerInvoker.size()).isEqualTo(2);
	}


	@Configuration
	@EnableIntegration
	public static class Config {

		@Bean
		public AmazonKinesis amazonKinesis() {
			AmazonKinesis amazonKinesis = mock(AmazonKinesis.class);

			given(amazonKinesis.describeStream(new DescribeStreamRequest().withStreamName(STREAM1)))
					.willReturn(new DescribeStreamResult()
									.withStreamDescription(new StreamDescription()
											.withStreamName(STREAM1)
											.withStreamStatus(StreamStatus.UPDATING)),
							new DescribeStreamResult()
									.withStreamDescription(new StreamDescription()
											.withStreamName(STREAM1)
											.withStreamStatus(StreamStatus.ACTIVE)
											.withHasMoreShards(false)
											.withShards(new Shard()
															.withShardId("1")
															.withSequenceNumberRange(new SequenceNumberRange()),
													new Shard()
															.withShardId("2")
															.withSequenceNumberRange(new SequenceNumberRange()),
													new Shard()
															.withShardId("3")
															.withSequenceNumberRange(new SequenceNumberRange()
																	.withEndingSequenceNumber("1")))));

			String shardIterator1 = "shardIterator1";

			given(amazonKinesis.getShardIterator(KinesisShardOffset.latest(STREAM1, "1").toShardIteratorRequest()))
					.willReturn(new GetShardIteratorResult()
							.withShardIterator(shardIterator1));

			String shardIterator2 = "shardIterator2";

			given(amazonKinesis.getShardIterator(KinesisShardOffset.latest(STREAM1, "2").toShardIteratorRequest()))
					.willReturn(new GetShardIteratorResult()
							.withShardIterator(shardIterator2));

			SerializingConverter serializingConverter = new SerializingConverter();

			String shardIterator11 = "shardIterator11";

			given(amazonKinesis.getRecords(new GetRecordsRequest()
					.withShardIterator(shardIterator1)
					.withLimit(25)))
					.willReturn(new GetRecordsResult()
							.withNextShardIterator(shardIterator11)
							.withRecords(new Record()
											.withPartitionKey("partition1")
											.withSequenceNumber("1")
											.withData(ByteBuffer.wrap(serializingConverter.convert("foo"))),
									new Record()
											.withPartitionKey("partition1")
											.withSequenceNumber("2")
											.withData(ByteBuffer.wrap(serializingConverter.convert("bar")))));

			given(amazonKinesis.getRecords(new GetRecordsRequest()
					.withShardIterator(shardIterator2)
					.withLimit(25)))
					.willReturn(new GetRecordsResult()
							.withNextShardIterator(shardIterator2));

			given(amazonKinesis.getRecords(new GetRecordsRequest()
					.withShardIterator(shardIterator11)
					.withLimit(25)))
					.willReturn(new GetRecordsResult()
							.withNextShardIterator(shardIterator11));

			given(amazonKinesis.getShardIterator(KinesisShardOffset.afterSequenceNumber(STREAM1, "1", "1")
					.toShardIteratorRequest()))
					.willReturn(new GetShardIteratorResult()
							.withShardIterator(shardIterator1));

			return amazonKinesis;
		}

		@Bean
		public MetadataStore checkpointStore() {
			SimpleMetadataStore simpleMetadataStore = new SimpleMetadataStore();
			String testKey = "SpringIntegration" + ":" + STREAM1 + ":" + "3";
			simpleMetadataStore.put(testKey, "1");
			return simpleMetadataStore;
		}

		@Bean
		public KinesisMessageDrivenChannelAdapter kinesisMessageDrivenChannelAdapter() {
			KinesisMessageDrivenChannelAdapter adapter =
					new KinesisMessageDrivenChannelAdapter(amazonKinesis(), STREAM1);
			adapter.setOutputChannel(kinesisChannel());
			adapter.setCheckpointStore(checkpointStore());
			adapter.setCheckpointMode(CheckpointMode.manual);
			adapter.setStartTimeout(10000);
			adapter.setDescribeStreamRetries(1);
			adapter.setConcurrency(10);

			DirectFieldAccessor dfa = new DirectFieldAccessor(adapter);
			dfa.setPropertyValue("describeStreamBackoff", 10);

			return adapter;
		}

		@Bean
		public PollableChannel kinesisChannel() {
			return new QueueChannel();
		}


	}

}
