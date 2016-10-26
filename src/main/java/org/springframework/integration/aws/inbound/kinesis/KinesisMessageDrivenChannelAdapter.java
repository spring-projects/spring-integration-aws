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

package org.springframework.integration.aws.inbound.kinesis;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.serializer.support.DeserializingConverter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.integration.aws.support.AwsHeaders;
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
import org.springframework.util.StringUtils;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.utils.NamedThreadFactory;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.StreamStatus;

/**
 * The {@link MessageProducerSupport} implementation for receiving data from Amazon Kinesis stream(s).
 *
 * @author Artem Bilan
 * @since 1.1
 */
@ManagedResource
@IntegrationManagedResource
public class KinesisMessageDrivenChannelAdapter extends MessageProducerSupport implements DisposableBean {

	private final AmazonKinesis amazonKinesis;

	private final String[] streams;

	private final Set<KinesisShardOffset> shardOffsets = new HashSet<>();

	private final Map<KinesisShardOffset, ShardConsumer> shardConsumers = new ConcurrentHashMap<>();

	private final Set<String> inResharding = new ConcurrentSkipListSet<>();

	private String consumerGroup = "SpringIntegration";

	private MetadataStore checkpointStore = new SimpleMetadataStore();

	private Executor dispatcherExecutor;

	private Executor consumerExecutor;

	private boolean consumerExecutorExplicitlySet;

	private KinesisShardOffset streamInitialSequence = KinesisShardOffset.latest();

	private Converter<byte[], Object> converter = new DeserializingConverter();

	private ListenerMode listenerMode = ListenerMode.record;

	private CheckpointMode checkpointMode = CheckpointMode.batch;

	private int recordsLimit = 25;

	private int consumerBackoff = 1000;

	private int startTimeout = 60 * 1000;

	private int describeStreamBackoff = 1000;

	private int describeStreamRetries = 50;

	private boolean resetCheckpoints;

	private volatile boolean active;

	public KinesisMessageDrivenChannelAdapter(AmazonKinesis amazonKinesis, String... streams) {
		Assert.notNull(amazonKinesis, "'amazonKinesis' must not be null.");
		Assert.notEmpty(streams, "'streams' must not be null.");
		this.amazonKinesis = amazonKinesis;
		this.streams = Arrays.copyOf(streams, streams.length);
	}

	public KinesisMessageDrivenChannelAdapter(AmazonKinesis amazonKinesis,
			KinesisShardOffset... shardOffsets) {
		Assert.notNull(amazonKinesis, "'amazonKinesis' must not be null.");
		Assert.notEmpty(shardOffsets, "'shardOffsets' must not be null.");
		Assert.noNullElements(shardOffsets, "'shardOffsets' must not contain null elements.");
		for (KinesisShardOffset shardOffset : shardOffsets) {
			Assert.isTrue(StringUtils.hasText(shardOffset.getStream()) && StringUtils.hasText(shardOffset.getShard()),
					"The 'shardOffsets' must be provided with particular 'stream' and 'shard' values.");
			this.shardOffsets.add(new KinesisShardOffset(shardOffset));
		}
		this.amazonKinesis = amazonKinesis;
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

	public void setExecutor(Executor executor) {
		Assert.notNull(executor, "'executor' must not be null");
		this.consumerExecutor = executor;
		this.dispatcherExecutor = executor;
		this.consumerExecutorExplicitlySet = true;
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

	public void setConsumerBackoff(int consumerBackoff) {
		this.consumerBackoff = Math.max(1000, consumerBackoff);
	}

	public void setDescribeStreamBackoff(int describeStreamBackoff) {
		this.describeStreamBackoff = Math.max(1000, describeStreamBackoff);
	}

	public void setDescribeStreamRetries(int describeStreamRetries) {
		Assert.isTrue(describeStreamRetries > 0, "'describeStreamRetries' must be more than 0");
		this.describeStreamRetries = describeStreamRetries;
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
			this.dispatcherExecutor =
					Executors.newSingleThreadExecutor(
							new NamedThreadFactory((getComponentName() == null
									? ""
									: getComponentName())
									+ "-kinesis-dispatcher-"));
		}
	}

	@Override
	public void destroy() throws Exception {
		if (!this.consumerExecutorExplicitlySet) {
			((ExecutorService) this.dispatcherExecutor).shutdownNow();
		}
	}

	@ManagedOperation
	public void stopConsumer(String stream, String shard) {
		ShardConsumer shardConsumer = this.shardConsumers.remove(KinesisShardOffset.latest(stream, shard));
		if (shardConsumer != null) {
			shardConsumer.stop();
		}
		else {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("There is no ShardConsumer for shard [" + shard + "] in stream [" + shard
						+ "] to stop.");
			}
		}
	}

	@ManagedOperation
	public void startConsumer(String stream, String shard) {
		KinesisShardOffset shardOffsetForSearch = KinesisShardOffset.latest(stream, shard);
		ShardConsumer shardConsumer = this.shardConsumers.get(shardOffsetForSearch);
		if (shardConsumer != null) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("The [" + shardConsumer + "] has been started before.");
			}
		}
		else {
			synchronized (this.shardOffsets) {
				for (KinesisShardOffset shardOffset : this.shardOffsets) {
					if (shardOffsetForSearch.equals(shardOffset)) {
						populateConsumer(shardOffset, shardOffset.isReset());
						break;
					}
				}
			}
		}
	}

	@ManagedOperation
	public void resetCheckpointForShardToLatest(String stream, String shard) {
		restartShardConsumerForOffset(KinesisShardOffset.latest(stream, shard));
	}

	@ManagedOperation
	public void resetCheckpointForShardToTrimHorizon(String stream, String shard) {
		restartShardConsumerForOffset(KinesisShardOffset.trimHorizon(stream, shard));
	}

	@ManagedOperation
	public void resetCheckpointForShardToSequenceNumber(String stream, String shard, String sequenceNumber) {
		restartShardConsumerForOffset(KinesisShardOffset.atSequenceNumber(stream, shard, sequenceNumber));
	}

	@ManagedOperation
	public void resetCheckpointForShardAtTimestamp(String stream, String shard, long timestamp) {
		restartShardConsumerForOffset(KinesisShardOffset.atTimestamp(stream, shard, new Date(timestamp)));
	}

	private void restartShardConsumerForOffset(KinesisShardOffset shardOffset) {
		Assert.isTrue(this.shardOffsets.contains(shardOffset),
				"The [" + this +
						"] doesn't operate shard [" + shardOffset.getShard() +
						"] for stream [" + shardOffset.getStream() + "]");

		if (logger.isDebugEnabled()) {
			logger.debug("Resetting consumer for [" + shardOffset + "]...");
		}
		shardOffset.reset();
		synchronized (this.shardOffsets) {
			this.shardOffsets.remove(shardOffset);
			this.shardOffsets.add(shardOffset);
		}
		if (this.active) {
			ShardConsumer oldShardConsumer = this.shardConsumers.remove(shardOffset);
			if (oldShardConsumer != null) {
				oldShardConsumer.close();
			}
			populateConsumer(shardOffset, true);
		}
	}

	@ManagedOperation
	public void resetCheckpoints() {
		this.resetCheckpoints = true;
		if (this.active) {
			stopConsumers();
			populateConsumers();
		}
	}

	@Override
	protected void doStart() {
		super.doStart();
		if (ListenerMode.batch.equals(this.listenerMode) && CheckpointMode.record.equals(this.checkpointMode)) {
			this.checkpointMode = CheckpointMode.batch;
			logger.warn("The 'checkpointMode' is overridden from [CheckpointMode.record] to [CheckpointMode.batch] " +
					"because it does not make sense in case of [ListenerMode.batch].");
		}
		if (this.streams != null) {
			populateShardsForStreams();
		}

		populateConsumers();

		this.active = true;
		this.dispatcherExecutor.execute(new ConsumerDispatcher());
	}

	private void populateShardsForStreams() {
		this.shardOffsets.clear();
		final CountDownLatch shardsGatherLatch = new CountDownLatch(this.streams.length);
		for (final String stream : this.streams) {
			populateShardsForStream(stream, shardsGatherLatch);
		}
		try {
			if (!shardsGatherLatch.await(this.startTimeout, TimeUnit.MILLISECONDS)) {
				throw new IllegalStateException("The [ "
						+ this +
						"] could not start during timeout: " + this.startTimeout);
			}
		}
		catch (InterruptedException e) {
			throw new IllegalStateException("The [ " + this + "] has been interrupted from start.");
		}
	}

	private void populateShardsForStream(final String stream, final CountDownLatch shardsGatherLatch) {
		this.consumerExecutor.execute(new Runnable() {

			@Override
			public void run() {
				try {
					int describeStreamRetries = 0;
					List<Shard> shards = new ArrayList<>();

					String exclusiveStartShardId = null;
					while (true) {
						DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest()
								.withStreamName(stream)
								.withExclusiveStartShardId(exclusiveStartShardId);

						DescribeStreamResult describeStreamResult = null;
						// Call DescribeStream, with backoff and retries (if we get LimitExceededException).
						try {
							describeStreamResult = KinesisMessageDrivenChannelAdapter.this.amazonKinesis
									.describeStream(describeStreamRequest);
						}
						catch (LimitExceededException e) {
							logger.info("Got LimitExceededException when describing stream [" + stream + "]. " +
									"Backing off for [" +
									KinesisMessageDrivenChannelAdapter.this.describeStreamBackoff + "] millis.");
						}

						if (describeStreamResult == null || !StreamStatus.ACTIVE.toString()
								.equals(describeStreamResult.getStreamDescription().getStreamStatus())) {
							if (describeStreamRetries++ >
									KinesisMessageDrivenChannelAdapter.this.describeStreamRetries) {
								ResourceNotFoundException resourceNotFoundException =
										new ResourceNotFoundException("The stream [" + stream +
												"] isn't ACTIVE or doesn't exist.");
								resourceNotFoundException.setServiceName("Kinesis");
								throw resourceNotFoundException;
							}
							try {
								Thread.sleep(KinesisMessageDrivenChannelAdapter.this.describeStreamBackoff);
								continue;
							}
							catch (InterruptedException e) {
								Thread.interrupted();
								throw new IllegalStateException("The [describeStream] thread for the stream ["
										+ stream + "] has been interrupted.", e);
							}
						}

						for (Shard shard : describeStreamResult.getStreamDescription().getShards()) {
							String endingSequenceNumber = shard.getSequenceNumberRange().getEndingSequenceNumber();
							if (endingSequenceNumber != null) {
								String key = KinesisMessageDrivenChannelAdapter.this.consumerGroup +
										":" + stream +
										":" + shard.getShardId();
								String checkpoint =
										KinesisMessageDrivenChannelAdapter.this.checkpointStore.get(key);

								if (checkpoint != null &&
										new BigInteger(endingSequenceNumber)
												.compareTo(new BigInteger(checkpoint)) <= 0) {
									// Skip CLOSED shard which has been read before according a checkpoint
									continue;
								}
							}

							shards.add(shard);
						}

						if (describeStreamResult.getStreamDescription().getHasMoreShards()) {
							exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
						}
						else {
							break;
						}
					}

					for (Shard shard : shards) {
						KinesisShardOffset shardOffset =
								new KinesisShardOffset(KinesisMessageDrivenChannelAdapter.this.streamInitialSequence);
						shardOffset.setShard(shard.getShardId());
						shardOffset.setStream(stream);
						boolean addedOffset;
						synchronized (KinesisMessageDrivenChannelAdapter.this.shardOffsets) {
							addedOffset = KinesisMessageDrivenChannelAdapter.this.shardOffsets.add(shardOffset);
						}
						if (addedOffset
								&& shardsGatherLatch == null
								&& KinesisMessageDrivenChannelAdapter.this.active) {
							populateConsumer(shardOffset, shardOffset.isReset());
						}
					}
				}
				finally {
					if (shardsGatherLatch != null) {
						shardsGatherLatch.countDown();
					}
					KinesisMessageDrivenChannelAdapter.this.inResharding.remove(stream);
				}
			}

		});
	}

	private void populateConsumers() {
		synchronized (this.shardOffsets) {
			for (KinesisShardOffset shardOffset : this.shardOffsets) {
				populateConsumer(shardOffset, this.resetCheckpoints);
			}
		}

		this.resetCheckpoints = false;
	}


	private void populateConsumer(KinesisShardOffset shardOffset, boolean reset) {
		shardOffset.setReset(reset);
		ShardConsumer shardConsumer = new ShardConsumer(shardOffset);
		this.shardConsumers.put(shardOffset, shardConsumer);
	}

	@Override
	protected void doStop() {
		this.active = false;
		super.doStop();
		stopConsumers();
	}

	private void stopConsumers() {
		for (ShardConsumer shardConsumer : this.shardConsumers.values()) {
			shardConsumer.stop();
		}
		this.shardConsumers.clear();
	}

	@Override
	public String toString() {
		return "KinesisMessageDrivenChannelAdapter{" +
				"shardOffsets=" + this.shardOffsets +
				", consumerGroup='" + this.consumerGroup + '\'' +
				'}';
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

	private final class ConsumerDispatcher implements SchedulingAwareRunnable {

		private final Set<String> inReshardingProcess = new HashSet<>();

		@Override
		public void run() {
			// We can't rely on the 'isRunning()' because of race condition,
			// when 'running' is set after submitting this task
			while (KinesisMessageDrivenChannelAdapter.this.active) {
				for (String stream : KinesisMessageDrivenChannelAdapter.this.inResharding) {
					// Local store to avoid several tasks for the same 'stream'
					if (this.inReshardingProcess.add(stream)) {
						if (logger.isDebugEnabled()) {
							logger.debug("Resharding has happened for stream [" + stream + "]. Rebalancing...");
						}
						populateShardsForStream(stream, null);
					}
				}

				for (Iterator<ShardConsumer> iterator =
						KinesisMessageDrivenChannelAdapter.this.shardConsumers.values().iterator();
						iterator.hasNext(); ) {
					ShardConsumer shardConsumer = iterator.next();
					shardConsumer.execute();
					if (ConsumerState.STOP == shardConsumer.state) {
						iterator.remove();
						if (KinesisMessageDrivenChannelAdapter.this.streams != null
								&& shardConsumer.shardIterator == null) {
							// Shard is CLOSED and we are capable for resharding
							KinesisShardOffset shardOffset = shardConsumer.shardOffset;
							String stream = shardOffset.getStream();
							if (KinesisMessageDrivenChannelAdapter.this.inResharding.add(stream)) {
								this.inReshardingProcess.remove(stream);
								synchronized (KinesisMessageDrivenChannelAdapter.this.shardOffsets) {
									KinesisMessageDrivenChannelAdapter.this.shardOffsets.remove(shardOffset);
								}
							}
						}
					}
				}
			}
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

	}

	private final class ShardConsumer {

		private final KinesisShardOffset shardOffset;

		private final ShardCheckpointer checkpointer;

		private final Runnable processTask = processTask();

		private volatile ConsumerState state = ConsumerState.NEW;

		private volatile Runnable task;

		private volatile String shardIterator;

		private volatile long sleepUntil;

		ShardConsumer(KinesisShardOffset shardOffset) {
			this.shardOffset = new KinesisShardOffset(shardOffset);
			String key = KinesisMessageDrivenChannelAdapter.this.consumerGroup +
					":" + shardOffset.getStream() +
					":" + shardOffset.getShard();
			this.checkpointer = new ShardCheckpointer(KinesisMessageDrivenChannelAdapter.this.checkpointStore, key);
		}

		void stop() {
			this.state = ConsumerState.STOP;
		}

		void close() {
			stop();
			this.checkpointer.close();
		}

		void execute() {
			if (this.task == null) {
				switch (this.state) {

					case NEW:
						this.task = new Runnable() {

							@Override
							public void run() {
								if (logger.isInfoEnabled()) {
									logger.info("The [" + this + "] has been started.");
								}
								if (ShardConsumer.this.shardOffset.isReset()) {
									ShardConsumer.this.checkpointer.remove();
								}
								else {
									String checkpoint = ShardConsumer.this.checkpointer.getCheckpoint();
									if (checkpoint != null) {
										ShardConsumer.this.shardOffset.setSequenceNumber(checkpoint);
										ShardConsumer.this.shardOffset
												.setIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
									}
								}

								GetShardIteratorRequest shardIteratorRequest =
										ShardConsumer.this.shardOffset.toShardIteratorRequest();
								ShardConsumer.this.shardIterator =
										KinesisMessageDrivenChannelAdapter.this.amazonKinesis
												.getShardIterator(shardIteratorRequest)
												.getShardIterator();
								if (ConsumerState.STOP != ShardConsumer.this.state) {
									ShardConsumer.this.state = ConsumerState.CONSUME;
								}
								ShardConsumer.this.task = null;
							}

						};
						break;

					case CONSUME:
						this.task = this.processTask;
						break;

					case SLEEP:
						if (System.currentTimeMillis() >= this.sleepUntil) {
							this.state = ConsumerState.CONSUME;
						}
						this.task = null;
						break;

					case STOP:
						if (this.shardIterator == null) {
							if (logger.isInfoEnabled()) {
								logger.info("Stopping the [" + this +
										"] because the shard has been CLOSED and exhausted.");
							}
						}
						else {
							if (logger.isInfoEnabled()) {
								logger.info("Stopping the [" + this + "].");
							}
						}
						this.task = null;
						break;
				}

				if (this.task != null) {
					KinesisMessageDrivenChannelAdapter.this.consumerExecutor.execute(this.task);
				}
			}
		}

		private Runnable processTask() {
			return new Runnable() {

				@Override
				public void run() {
					GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
					getRecordsRequest.setShardIterator(ShardConsumer.this.shardIterator);
					getRecordsRequest.setLimit(KinesisMessageDrivenChannelAdapter.this.recordsLimit);
					GetRecordsResult result = KinesisMessageDrivenChannelAdapter.this.amazonKinesis
							.getRecords(getRecordsRequest);

					List<Record> records = result.getRecords();

					if (!records.isEmpty()) {
						processRecords(records);
					}

					ShardConsumer.this.shardIterator = result.getNextShardIterator();

					if (ShardConsumer.this.shardIterator == null) {
						ShardConsumer.this.state = ConsumerState.STOP;
					}

					if (ConsumerState.STOP != ShardConsumer.this.state && records.isEmpty()) {
						if (logger.isDebugEnabled()) {
							logger.debug("No records for [" + ShardConsumer.this
									+ "] on sequenceNumber [" + ShardConsumer.this.checkpointer.lastCheckpointValue
									+ "]. Suspend consuming for ["
									+ KinesisMessageDrivenChannelAdapter.this.consumerBackoff + "] milliseconds.");
						}
						ShardConsumer.this.sleepUntil = System.currentTimeMillis() +
								KinesisMessageDrivenChannelAdapter.this.consumerBackoff;
						ShardConsumer.this.state = ConsumerState.SLEEP;
					}

					ShardConsumer.this.task = null;
				}

			};
		}

		private void processRecords(List<Record> records) {
			records = this.checkpointer.filterRecords(records);
			if (!records.isEmpty()) {
				if (logger.isDebugEnabled()) {
					logger.debug("Processing records: " + records + " for [" + this + "]");
				}
				switch (KinesisMessageDrivenChannelAdapter.this.listenerMode) {
					case record:
						for (Record record : records) {
							Object payload =
									KinesisMessageDrivenChannelAdapter.this.converter.convert(record.getData().array());
							AbstractIntegrationMessageBuilder<Object> messageBuilder = getMessageBuilderFactory()
									.withPayload(payload)
									.setHeader(AwsHeaders.STREAM, this.shardOffset.getStream())
									.setHeader(AwsHeaders.SHARD, this.shardOffset.getShard())
									.setHeader(AwsHeaders.PARTITION_KEY, record.getPartitionKey())
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
								.setHeader(AwsHeaders.SHARD, this.shardOffset.getShard());
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
		public String toString() {
			return "ShardConsumer{" +
					"shardOffset=" + this.shardOffset +
					", state=" + this.state +
					'}';
		}

	}

	private enum ConsumerState {

		NEW, CONSUME, SLEEP, STOP

	}

	private static class ShardCheckpointer implements Checkpointer {

		private static final Log logger = LogFactory.getLog(ShardCheckpointer.class);

		private final MetadataStore checkpointStore;

		private final String key;

		private volatile String lastCheckpointValue;

		private volatile boolean active = true;

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
						if (logger.isDebugEnabled()) {
							logger.debug("Removing record with sequenceNumber " + sequenceNumber +
									" because the sequenceNumber is <= checkpoint(" + this.lastCheckpointValue + ")");
						}
						iterator.remove();
					}
					else {
						this.lastCheckpointValue = sequenceNumber;
					}
				}

			}
			else {
				this.lastCheckpointValue = recordsToProcess.get(recordsToProcess.size() - 1).getSequenceNumber();
			}
			return recordsToProcess;
		}

		@Override
		public void checkpoint() {
			checkpoint(this.lastCheckpointValue);
		}

		@Override
		public void checkpoint(String sequenceNumber) {
			if (this.active) {
				String existingSequence = this.checkpointStore.get(this.key);
				if (existingSequence == null ||
						new BigInteger(existingSequence).compareTo(new BigInteger(sequenceNumber)) <= 0) {
					this.checkpointStore.put(this.key, sequenceNumber);
				}
			}
			else {
				if (logger.isInfoEnabled()) {
					logger.info("The [" + this + "] has been closed. Checkpoints aren't accepted anymore.");
				}
			}
		}

		private String getCheckpoint() {
			return this.checkpointStore.get(this.key);
		}

		private void remove() {
			this.checkpointStore.remove(this.key);
		}

		private void close() {
			this.active = false;
		}

		@Override
		public String toString() {
			return "ShardCheckpointer{" +
					"key='" + this.key + '\'' +
					", lastCheckpointValue='" + this.lastCheckpointValue + '\'' +
					'}';
		}

	}

}
