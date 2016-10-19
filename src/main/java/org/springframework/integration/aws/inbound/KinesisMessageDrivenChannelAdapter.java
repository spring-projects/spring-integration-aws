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

package org.springframework.integration.aws.inbound;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.serializer.support.DeserializingConverter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.aws.support.kinesis.Checkpointer;
import org.springframework.integration.aws.support.kinesis.KinesisShardOffset;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.metadata.MetadataStore;
import org.springframework.integration.metadata.SimpleMetadataStore;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.integration.support.management.IntegrationManagedResource;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.messaging.Message;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.util.Assert;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamStatus;

/**
 * The {@link MessageProducerSupport} implementation for receiving data from Amazon Kinesis stream(s).
 *
 * @author Artem Bilan
 * @since 1.1
 */
@ManagedResource
@IntegrationManagedResource
public class KinesisMessageDrivenChannelAdapter extends MessageProducerSupport {

	private final AmazonKinesis amazonKinesis;

	private final String[] streams;

	private final Set<KinesisShardOffset> shardOffsets = new HashSet<>();

	private final Map<KinesisShardOffset, ShardConsumer> shardConsumers = new ConcurrentHashMap<>();

	private String consumerGroup = "SpringIntegration";

	private MetadataStore checkpointStore = new SimpleMetadataStore();

	private Executor consumerExecutor;

	private KinesisShardOffset streamInitialSequence = KinesisShardOffset.latest();

	private Converter<byte[], Object> converter = new DeserializingConverter();

	private ListenerMode listenerMode = ListenerMode.record;

	private CheckpointMode checkpointMode = CheckpointMode.batch;

	private int recordsLimit = 25;

	private int consumerSleepInterval = 1000;

	private int startTimeout = 60 * 1000;

	private boolean resetShards;

	public KinesisMessageDrivenChannelAdapter(AmazonKinesis amazonKinesis, String... streams) {
		Assert.notNull(amazonKinesis, "'amazonKinesis' must not be null.");
		Assert.notEmpty(streams, "'streams' must not be null.");
		this.amazonKinesis = amazonKinesis;
		this.streams = Arrays.copyOf(streams, streams.length);
	}

	public KinesisMessageDrivenChannelAdapter(AmazonKinesis amazonKinesis,
			KinesisShardOffset... shardOffsets) {
		Assert.notNull(amazonKinesis, "'amazonKinesis' must not be null.");
		Assert.notEmpty(shardOffsets, "'shardInitialSequences' must not be null.");
		this.amazonKinesis = amazonKinesis;
		Collections.addAll(this.shardOffsets, shardOffsets);
		this.streams = null;
	}

	public void setConsumerGroup(String consumerGroup) {
		Assert.hasText(consumerGroup, "'consumerGroup' must not be empty");
		this.consumerGroup = consumerGroup;
	}

	public void setCheckpointStore(MetadataStore checkpointStore) {
		Assert.notNull(checkpointStore, "'checkpointStore' must not be null");
		this.checkpointStore = checkpointStore;
	}

	public void setConsumerExecutor(Executor consumerExecutor) {
		Assert.notNull(consumerExecutor, "'consumerExecutor' must not be null");
		this.consumerExecutor = consumerExecutor;
	}

	public void setStreamInitialSequence(KinesisShardOffset streamInitialSequence) {
		Assert.notNull(streamInitialSequence, "'streamInitialSequence' must not be null");
		this.streamInitialSequence = streamInitialSequence;
	}

	public void setConverter(Converter<byte[], Object> converter) {
		Assert.notNull(converter, "'converter' must not be null");
		this.converter = converter;
	}

	public void setListenerMode(ListenerMode listenerMode) {
		Assert.notNull(listenerMode, "'listenerMode' must not be null");
		this.listenerMode = listenerMode;
	}

	public void setCheckpointMode(CheckpointMode checkpointMode) {
		Assert.notNull(checkpointMode, "'checkpointMode' must not be null");
		this.checkpointMode = checkpointMode;
	}

	public void setRecordsLimit(int recordsLimit) {
		Assert.isTrue(recordsLimit > 0, "'recordsLimit' must be more than 0");
		this.recordsLimit = recordsLimit;
	}

	public void setConsumerSleepInterval(int consumerSleepInterval) {
		this.consumerSleepInterval = Math.max(1000, consumerSleepInterval);
	}

	public void setStartTimeout(int startTimeout) {
		Assert.isTrue(startTimeout > 0, "'startTimeout' must be more than 0");
		this.startTimeout = startTimeout;
	}

	@Override
	protected void onInit() {
		super.onInit();

		if (this.consumerExecutor == null) {
			this.consumerExecutor = new SimpleAsyncTaskExecutor(
					(getComponentName() == null ? "" : getComponentName()) + "-kinesis-consumer-");
		}

		if (ListenerMode.batch.equals(this.listenerMode) && CheckpointMode.record.equals(this.checkpointMode)) {
			this.checkpointMode = CheckpointMode.batch;
			logger.warn("The 'checkpointMode' is overridden from [CheckpointMode.record] to [CheckpointMode.batch] " +
					"because it does not make sense in case of [ListenerMode.batch].");
		}

	}

	@ManagedOperation
	public void resetShards() {
		if (isRunning()) {
			stop();
			start();
		}
		else {
			this.resetShards = true;
		}
	}

	@Override
	protected void doStart() {
		super.doStart();
		if (this.streams != null) {
			populateShardsForStreams();
		}

		if (this.resetShards) {
			for (KinesisShardOffset shardOffset : this.shardOffsets) {
				shardOffset.reset();
			}
		}
	}

	private void populateShardsForStreams() {
		this.shardOffsets.clear();
		final CountDownLatch shardsGatherLatch = new CountDownLatch(this.streams.length);
		for (final String stream : this.streams) {
			this.consumerExecutor.execute(new Runnable() {

				@Override
				public void run() {
					try {
						int describeStreamRetries = 0;
						List<Shard> shards = new ArrayList<>();
						String exclusiveStartShardId = null;
						do {
							DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
							describeStreamRequest.setStreamName(stream);
							describeStreamRequest.setExclusiveStartShardId(exclusiveStartShardId);
							DescribeStreamResult describeStreamResult =
									KinesisMessageDrivenChannelAdapter.this.amazonKinesis
											.describeStream(describeStreamRequest);

							if (!StreamStatus.ACTIVE.toString()
									.equals(describeStreamResult.getStreamDescription().getStreamStatus())) {
								if (describeStreamRetries++ > 3) {
									throw new ResourceNotFoundException("The stream [" + stream +
											"] isn't ACTIVE or doesn't exist.");
								}
								try {
									Thread.sleep(1000);
									continue;
								}
								catch (InterruptedException e) {
									Thread.interrupted();
									throw new IllegalStateException("The [describeStream] thread for the stream ["
											+ stream + "] has been interrupted.", e);
								}
							}

							shards.addAll(describeStreamResult.getStreamDescription().getShards());
							if (describeStreamResult.getStreamDescription().getHasMoreShards()
									&& shards.size() > 0) {
								exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
							}
							else {
								exclusiveStartShardId = null;
							}
						}
						while (exclusiveStartShardId != null);

						KinesisShardOffset streamInitialSequence =
								KinesisMessageDrivenChannelAdapter.this.streamInitialSequence;
						for (Shard shard : shards) {
							KinesisShardOffset shardOffset = new KinesisShardOffset(streamInitialSequence);
							shardOffset.setShard(shard.getShardId());
							KinesisMessageDrivenChannelAdapter.this.shardOffsets.add(shardOffset);
						}
					}
					finally {
						shardsGatherLatch.countDown();
					}
				}

			});
		}
		try {
			shardsGatherLatch.await(this.startTimeout, TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
			throw new IllegalStateException("The KinesisMessageDrivenChannelAdapter [ "
					+ this +
					"] could not start during timeout: " + this.startTimeout);
		}
	}

	@Override
	protected void doStop() {
		super.doStop();
		for (ShardConsumer shardConsumer : this.shardConsumers.values()) {
			shardConsumer.stop();
		}
		this.shardConsumers.clear();
	}

	/**
	 * The listener mode, record or batch.
	 */
	public enum ListenerMode {

		/**
		 * Each {@link Message} will be converted from a single {@code Record}.
		 */
		record,

		/**
		 * Each {@link Message} will contains {@code List<Record>} if not empty.
		 */
		batch
	}

	/**
	 * The listener mode, record or batch.
	 */
	public enum CheckpointMode {

		/**
		 * Checkpoint after each processed record.
		 * Makes sense only if {@link ListenerMode#record} is used.
		 */
		record,

		/**
		 * Checkpoint after each processed batch of records.
		 */
		batch,

		/**
		 * Checkpoint on demand via provided to the message {@link Checkpointer} callback.
		 */
		manual
	}

	private final class ShardConsumer implements SchedulingAwareRunnable {

		private final KinesisShardOffset shardOffset;

		private final ShardCheckpointer checkpointer;

		private volatile boolean active = true;

		private ShardConsumer(KinesisShardOffset shardOffset) {
			this.shardOffset = shardOffset;
			String key = KinesisMessageDrivenChannelAdapter.this.consumerGroup +
					":" + shardOffset.getStream() +
					":" + shardOffset.getShard();
			this.checkpointer =
					new ShardCheckpointer(KinesisMessageDrivenChannelAdapter.this.checkpointStore, key);
		}

		public void stop() {
			this.active = false;
		}

		@Override
		public void run() {
			try {
				GetShardIteratorRequest shardIteratorRequest = this.shardOffset.toGetShardIteratorRequest();
				AmazonKinesis client = KinesisMessageDrivenChannelAdapter.this.amazonKinesis;
				String shardIterator = client.getShardIterator(shardIteratorRequest)
						.getShardIterator();

				while (this.active) {
					GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
					getRecordsRequest.setShardIterator(shardIterator);
					getRecordsRequest.setLimit(KinesisMessageDrivenChannelAdapter.this.recordsLimit);
					GetRecordsResult result = client.getRecords(getRecordsRequest);

					List<Record> records = result.getRecords();

					processRecords(records);

					shardIterator = result.getNextShardIterator();

					if (shardIterator == null) {
						break;
					}

					if (this.active && records.isEmpty()) {
						try {
							Thread.sleep(KinesisMessageDrivenChannelAdapter.this.consumerSleepInterval);
						}
						catch (InterruptedException exception) {
							throw new RuntimeException(exception);
						}
					}
				}

				if (this.active) {
					if (logger.isInfoEnabled()) {
						logger.info("Stopping the ShardConsumer for the shard [" + this.shardOffset.getShard() +
								"] in stream [" + this.shardOffset.getStream() +
								"] because the shard has been CLOSED.");
					}
					this.active = false;
				}
				else {
					if (logger.isInfoEnabled()) {
						logger.info("Stopping the ShardConsumer for the shard [" + this.shardOffset.getShard() +
								"] in stream [" + this.shardOffset.getStream() + "].");
					}
				}
			}
			finally {
				KinesisMessageDrivenChannelAdapter.this.shardConsumers.remove(this.shardOffset);
			}
		}

		private void processRecords(List<Record> records) {
			records = this.checkpointer.filterRecords(records);
			if (!records.isEmpty()) {
				switch (KinesisMessageDrivenChannelAdapter.this.listenerMode) {
					case record:
						for (Record record : records) {
							Object payload =
									KinesisMessageDrivenChannelAdapter.this.converter.convert(record.getData().array());
							AbstractIntegrationMessageBuilder<Object> messageBuilder = getMessageBuilderFactory()
									.withPayload(payload)
									.setHeader(AwsHeaders.STREAM, this.shardOffset.getStream())
									.setHeader(AwsHeaders.PARTITION_KEY, this.shardOffset.getShard())
									.setHeader(AwsHeaders.SEQUENCE_NUMBER, record.getSequenceNumber());
							if (CheckpointMode.manual.equals(KinesisMessageDrivenChannelAdapter.this.checkpointMode)) {
								messageBuilder.setHeader(AwsHeaders.CHECKPOINTER, this.checkpointer);
							}

							sendMessage(messageBuilder.build());

							if (CheckpointMode.record.equals(KinesisMessageDrivenChannelAdapter.this.checkpointMode)) {
								this.checkpointer.checkpoint(record.getSequenceNumber());
							}
						}
						break;

					case batch:
						AbstractIntegrationMessageBuilder<?> messageBuilder = getMessageBuilderFactory()
								.withPayload(records)
								.setHeader(AwsHeaders.STREAM, this.shardOffset.getStream())
								.setHeader(AwsHeaders.PARTITION_KEY, this.shardOffset.getShard());
						if (CheckpointMode.manual.equals(KinesisMessageDrivenChannelAdapter.this.checkpointMode)) {
							messageBuilder.setHeader(AwsHeaders.CHECKPOINTER, this.checkpointer);
						}

						sendMessage(messageBuilder.build());

						break;

				}

				if (CheckpointMode.batch.equals(KinesisMessageDrivenChannelAdapter.this.checkpointMode)) {
					this.checkpointer.checkpoint();
				}
			}
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

	}

	private static class ShardCheckpointer implements Checkpointer {

		private static final Log logger = LogFactory.getLog(ShardCheckpointer.class);

		private final MetadataStore checkpointStore;

		private final String key;

		private volatile String lastCheckpointValue;

		ShardCheckpointer(MetadataStore checkpointStore, String key) {
			this.checkpointStore = checkpointStore;
			this.key = key;
		}

		List<Record> filterRecords(List<Record> records) {
			List<Record> recordsToProcess = new LinkedList<>(records);
			this.lastCheckpointValue = this.checkpointStore.get(this.key);
			if (this.lastCheckpointValue != null) {
				for (Iterator<Record> iterator = recordsToProcess.iterator(); iterator.hasNext(); ) {
					Record record = iterator.next();
					String sequenceNumber = record.getSequenceNumber();
					if (new BigInteger(sequenceNumber).compareTo(new BigInteger(this.lastCheckpointValue)) <= 0) {
						iterator.remove();
						if (logger.isDebugEnabled()) {
							logger.debug("Removing record with sequenceNumber " +
									sequenceNumber +
									" because the sequenceNumber is <= checkpoint (" +
									this.lastCheckpointValue + ")");
						}
					}
					else {
						this.lastCheckpointValue = sequenceNumber;
					}
				}

			}
			return recordsToProcess;
		}

		@Override
		public void checkpoint() {
			checkpoint(this.lastCheckpointValue);
		}

		@Override
		public void checkpoint(String sequenceNumber) {
			this.checkpointStore.put(this.key, sequenceNumber);
		}

	}

}
