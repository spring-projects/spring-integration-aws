/*
 * Copyright 2017-2018 the original author or authors.
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
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.AttributeAccessor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.serializer.support.DeserializingConverter;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.mapping.InboundMessageMapper;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.metadata.SimpleMetadataStore;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageUtils;
import org.springframework.integration.support.locks.LockRegistry;
import org.springframework.integration.support.management.IntegrationManagedResource;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.messaging.Message;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.StreamStatus;

/**
 * The {@link MessageProducerSupport} implementation for receiving data from Amazon Kinesis stream(s).
 *
 * @author Artem Bilan
 * @author Krzysztof Witkowski
 *
 * @since 1.1
 */
@ManagedResource
@IntegrationManagedResource
public class KinesisMessageDrivenChannelAdapter extends MessageProducerSupport implements DisposableBean {

	private static final ThreadLocal<AttributeAccessor> attributesHolder = new ThreadLocal<>();

	private final AmazonKinesis amazonKinesis;

	private final String[] streams;

	private final Set<KinesisShardOffset> shardOffsets = new HashSet<>();

	private final Map<KinesisShardOffset, ShardConsumer> shardConsumers = new ConcurrentHashMap<>();

	private final Set<String> inResharding = new ConcurrentSkipListSet<>();

	private final List<ConsumerInvoker> consumerInvokers = new ArrayList<>();

	private final ShardConsumerManager shardConsumerManager = new ShardConsumerManager();

	private final ExecutorService shardLocksExecutor =
			Executors.newSingleThreadExecutor(
					new CustomizableThreadFactory(
							(getComponentName() == null
									? ""
									: getComponentName())
									+ "-kinesis-shard-locks-"));

	private String consumerGroup = "SpringIntegration";

	private ConcurrentMetadataStore checkpointStore = new SimpleMetadataStore();

	private Executor dispatcherExecutor;

	private boolean dispatcherExecutorExplicitlySet;

	private Executor consumerExecutor;

	private boolean consumerExecutorExplicitlySet;

	private int maxConcurrency;

	private int concurrency;

	private KinesisShardOffset streamInitialSequence = KinesisShardOffset.latest();

	private Converter<byte[], Object> converter = new DeserializingConverter();

	private ListenerMode listenerMode = ListenerMode.record;

	private CheckpointMode checkpointMode = CheckpointMode.batch;

	private int recordsLimit = 10000;

	private int idleBetweenPolls = 1000;

	private int consumerBackoff = 1000;

	private int startTimeout = 60 * 1000;

	private int describeStreamBackoff = 1000;

	private int describeStreamRetries = 50;

	private boolean resetCheckpoints;

	private InboundMessageMapper<byte[]> embeddedHeadersMapper;

	private LockRegistry lockRegistry;

	private volatile boolean active;

	private volatile int consumerInvokerMaxCapacity;

	private volatile Future<?> shardConsumerManagerFuture;

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

	public void setCheckpointStore(ConcurrentMetadataStore checkpointStore) {
		Assert.notNull(checkpointStore, "'checkpointStore' must not be null");
		this.checkpointStore = checkpointStore;
	}

	public void setConsumerExecutor(Executor executor) {
		Assert.notNull(executor, "'executor' must not be null");
		this.consumerExecutor = executor;
		this.consumerExecutorExplicitlySet = true;
	}

	public void setDispatcherExecutor(Executor dispatcherExecutor) {
		this.dispatcherExecutor = dispatcherExecutor;
		this.dispatcherExecutorExplicitlySet = true;
	}

	public void setStreamInitialSequence(KinesisShardOffset streamInitialSequence) {
		Assert.notNull(streamInitialSequence, "'streamInitialSequence' must not be null");
		this.streamInitialSequence = streamInitialSequence;
	}

	/**
	 * Specify a {@link Converter} to deserialize the {@code byte[]} from record's body.
	 * Can be {@code null} meaning no deserialization.
	 * @param converter the {@link Converter} to use or null
	 */
	public void setConverter(Converter<byte[], Object> converter) {
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

	/**
	 * The maximum record to poll per on get-records request.
	 * Not greater then {@code 10000}.
	 * @param recordsLimit the number of records to for per on get-records request.
	 * @see GetRecordsRequest#setLimit
	 */
	public void setRecordsLimit(int recordsLimit) {
		Assert.isTrue(recordsLimit > 0, "'recordsLimit' must be more than 0");
		this.recordsLimit = Math.min(10000, recordsLimit);
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

	/**
	 * The maximum number of concurrent {@link ConsumerInvoker}s running.
	 * The {@link ShardConsumer}s are evenly distributed between {@link ConsumerInvoker}s.
	 * Messages from within the same shard will be processed sequentially.
	 * In other words each shard is tied with the particular thread.
	 * By default the concurrency is unlimited and shard
	 * is processed in the {@link #consumerExecutor} directly.
	 * @param concurrency the concurrency maximum number
	 */
	public void setConcurrency(int concurrency) {
		this.maxConcurrency = concurrency;
	}

	/**
	 * The sleep interval in milliseconds used in the main loop between shards polling cycles.
	 * Defaults to {@code 1000}l minimum {@code 250}.
	 * @param idleBetweenPolls the interval to sleep between shards polling cycles.
	 */
	public void setIdleBetweenPolls(int idleBetweenPolls) {
		this.idleBetweenPolls = Math.max(250, idleBetweenPolls);
	}

	/**
	 * Specify an {@link InboundMessageMapper} to extract message headers embedded into the record data.
	 * @param embeddedHeadersMapper the {@link InboundMessageMapper} to use.
	 * @since 2.0
	 */
	public void setEmbeddedHeadersMapper(InboundMessageMapper<byte[]> embeddedHeadersMapper) {
		this.embeddedHeadersMapper = embeddedHeadersMapper;
	}

	/**
	 * Specify a {@link LockRegistry} for an exclusive access to provided streams.
	 * This is not used when shards-based configuration is provided.
	 * @param lockRegistry the {@link LockRegistry} to use.
	 * @since 2.0
	 */
	public void setLockRegistry(LockRegistry lockRegistry) {
		this.lockRegistry = lockRegistry;
	}

	@Override
	protected void onInit() {
		super.onInit();

		if (this.consumerExecutor == null) {
			this.consumerExecutor = Executors.newCachedThreadPool(
					new CustomizableThreadFactory((getComponentName() == null
							? ""
							: getComponentName())
							+ "-kinesis-consumer-"));
		}
		if (this.dispatcherExecutor == null) {
			this.dispatcherExecutor =
					Executors.newCachedThreadPool(
							new CustomizableThreadFactory((getComponentName() == null
									? ""
									: getComponentName())
									+ "-kinesis-dispatcher-"));
		}

		if (this.streams == null) {
			if (this.lockRegistry != null) {
				logger.warn("The LockRegistry is ignored when explicit shards configuration is used.");
			}
			this.lockRegistry = null;
		}
	}

	@Override
	public void destroy() {
		if (!this.consumerExecutorExplicitlySet) {
			((ExecutorService) this.consumerExecutor).shutdown();
		}
		if (!this.dispatcherExecutorExplicitlySet) {
			((ExecutorService) this.dispatcherExecutor).shutdown();
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
						this.shardConsumerManager.addShardToConsume(shardOffset);
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
				"The [" + KinesisMessageDrivenChannelAdapter.this +
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
			shardOffset.setReset(true);
			this.shardConsumerManager.addShardToConsume(shardOffset);
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

		this.concurrency = Math.min(this.maxConcurrency, this.shardOffsets.size());

		this.dispatcherExecutor.execute(new ConsumerDispatcher());

		this.shardConsumerManagerFuture = this.shardLocksExecutor.submit(this.shardConsumerManager);
	}

	private Collection<ShardConsumer> shardConsumerSubset(int i) {
		List<ShardConsumer> shardConsumers = new ArrayList<>(this.shardConsumers.values());
		if (this.concurrency == 1) {
			return shardConsumers;
		}
		else {
			int numConsumers = shardConsumers.size();
			if (numConsumers == this.concurrency) {
				return Collections.singleton(shardConsumers.get(i));
			}
			else {
				int perInvoker = numConsumers / this.concurrency;
				List<ShardConsumer> subset;
				if (i == this.concurrency - 1) {
					subset = shardConsumers.subList(i * perInvoker, numConsumers);
				}
				else {
					subset = shardConsumers.subList(i * perInvoker, (i + 1) * perInvoker);
				}
				return subset;
			}
		}
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
						+ KinesisMessageDrivenChannelAdapter.this +
						"] could not start during timeout: " + this.startTimeout);
			}
		}
		catch (InterruptedException e) {
			throw new IllegalStateException("The [ " + KinesisMessageDrivenChannelAdapter.this +
					"] has been interrupted from start.");
		}
	}

	private void populateShardsForStream(final String stream, final CountDownLatch shardsGatherLatch) {
		this.dispatcherExecutor.execute(() -> {
			try {
				int describeStreamRetries = 0;
				List<Shard> shardsToConsume = new ArrayList<>();

				String exclusiveStartShardId = null;
				while (true) {
					DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest()
							.withStreamName(stream)
							.withExclusiveStartShardId(exclusiveStartShardId);

					DescribeStreamResult describeStreamResult = null;
					// Call DescribeStream, with backoff and retries (if we get LimitExceededException).
					try {
						describeStreamResult = this.amazonKinesis.describeStream(describeStreamRequest);
					}
					catch (Exception e) {
						logger.info("Got an exception when describing stream [" + stream + "]. " +
								"Backing off for [" + this.describeStreamBackoff + "] millis.", e);
					}

					if (describeStreamResult == null ||
							!StreamStatus.ACTIVE.toString().equals(describeStreamResult.getStreamDescription().getStreamStatus())) {

						if (describeStreamRetries++ > this.describeStreamRetries) {
							ResourceNotFoundException resourceNotFoundException =
									new ResourceNotFoundException("The stream [" + stream +
											"] isn't ACTIVE or doesn't exist.");
							resourceNotFoundException.setServiceName("Kinesis");
							throw resourceNotFoundException;
						}
						try {
							Thread.sleep(this.describeStreamBackoff);
							continue;
						}
						catch (InterruptedException e) {
							Thread.interrupted();
							throw new IllegalStateException("The [describeStream] thread for the stream ["
									+ stream + "] has been interrupted.", e);
						}
					}

					List<Shard> shards = describeStreamResult.getStreamDescription().getShards();

					try {
						for (Shard shard : shards) {
							String key = buildCheckpointKeyForShard(stream, shard.getShardId());
							String endingSequenceNumber = shard.getSequenceNumberRange().getEndingSequenceNumber();
							if (endingSequenceNumber != null) {
								String checkpoint = this.checkpointStore.get(key);

								boolean skipClosedShard = checkpoint != null &&
										new BigInteger(endingSequenceNumber)
												.compareTo(new BigInteger(checkpoint)) <= 0;

								if (logger.isTraceEnabled()) {
									logger.trace("The shard [" + shard + "] in stream [" + stream +
											"] is closed CLOSED with endingSequenceNumber [" + endingSequenceNumber +
											"].\nThe last processed checkpoint is [" + checkpoint + "]." +
											(skipClosedShard ? "\nThe shard will be skipped." : ""));
								}

								if (skipClosedShard) {
									// Skip CLOSED shard which has been read before according a checkpoint
									continue;
								}
							}

							shardsToConsume.add(shard);
						}
					}
					catch (Exception e) {
						logger.info("Got an exception when processing shards in stream [" + stream + "].\n" +
								"Retrying...", e);
						continue;
					}

					if (describeStreamResult.getStreamDescription().getHasMoreShards()) {
						exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
						describeStreamRetries = 0;
					}
					else {
						break;
					}
				}

				for (Shard shard : shardsToConsume) {
					KinesisShardOffset shardOffset = new KinesisShardOffset(this.streamInitialSequence);
					shardOffset.setShard(shard.getShardId());
					shardOffset.setStream(stream);
					boolean addedOffset;
					synchronized (this.shardOffsets) {
						addedOffset = this.shardOffsets.add(shardOffset);
					}
					if (addedOffset && shardsGatherLatch == null && this.active) {
						this.shardConsumerManager.addShardToConsume(shardOffset);
					}
				}
			}
			finally {
				if (shardsGatherLatch != null) {
					shardsGatherLatch.countDown();
				}
				this.inResharding.remove(stream);
			}
		});
	}

	private void populateConsumers() {
		synchronized (this.shardOffsets) {
			for (KinesisShardOffset shardOffset : this.shardOffsets) {
				this.shardConsumerManager.addShardToConsume(shardOffset);
			}
		}

		this.resetCheckpoints = false;
	}

	private void populateConsumer(KinesisShardOffset shardOffset) {
		shardOffset.setReset(this.resetCheckpoints);
		ShardConsumer shardConsumer = new ShardConsumer(shardOffset);

		if (this.active) {
			synchronized (this.consumerInvokers) {
				if (this.consumerInvokers.size() < this.maxConcurrency) {
					ConsumerInvoker consumerInvoker = new ConsumerInvoker(Collections.singleton(shardConsumer));
					this.consumerInvokers.add(consumerInvoker);
					this.consumerExecutor.execute(consumerInvoker);
				}
				else {
					for (ConsumerInvoker consumerInvoker : this.consumerInvokers) {
						if (consumerInvoker.consumers.size() < this.consumerInvokerMaxCapacity) {
							consumerInvoker.addConsumer(shardConsumer);
							return;
						}
					}

					if (this.concurrency != 0) {
						ConsumerInvoker firstConsumerInvoker = this.consumerInvokers.get(0);
						firstConsumerInvoker.addConsumer(shardConsumer);
						this.consumerInvokerMaxCapacity = firstConsumerInvoker.consumers.size();
					}
				}
			}
		}

		this.shardConsumers.put(shardOffset, shardConsumer);
	}

	private String buildCheckpointKeyForShard(String stream, String shardId) {
		return this.consumerGroup + ":" + stream + ":" + shardId;
	}

	@Override
	protected void doStop() {
		for (ConsumerInvoker consumerInvoker : this.consumerInvokers) {
			consumerInvoker.notifyBarrier();
		}
		super.doStop();
		stopConsumers();

		this.shardConsumerManagerFuture.cancel(true);
		this.active = false;
	}

	private void stopConsumers() {
		for (ShardConsumer shardConsumer : this.shardConsumers.values()) {
			shardConsumer.stop();
		}
		this.shardConsumers.clear();
	}

	/**
	 * If there's an error channel, we create a new attributes holder here.
	 * Then set the attributes for use by the {@link ErrorMessageStrategy}.
	 * @param record the Kinesis record to use.
	 * @param message the Spring Messaging message to use.
	 */
	private void setAttributesIfNecessary(Object record, Message<?> message) {
		if (getErrorChannel() != null) {
			AttributeAccessor attributes = ErrorMessageUtils.getAttributeAccessor(message, null);
			attributesHolder.set(attributes);
			attributes.setAttribute(AwsHeaders.RAW_RECORD, record);
		}
	}

	@Override
	protected AttributeAccessor getErrorMessageAttributes(org.springframework.messaging.Message<?> message) {
		AttributeAccessor attributes = attributesHolder.get();
		if (attributes == null) {
			return super.getErrorMessageAttributes(message);
		}
		else {
			return attributes;
		}
	}

	@Override
	public String toString() {
		return "KinesisMessageDrivenChannelAdapter{" +
				"shardOffsets=" + this.shardOffsets +
				", consumerGroup='" + this.consumerGroup + '\'' +
				'}';
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

				try {
					Thread.sleep(KinesisMessageDrivenChannelAdapter.this.idleBetweenPolls);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException("ConsumerDispatcher Thread [" + this + "] has been interrupted", e);
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

		private final String key;

		private Runnable notifier;

		private volatile ConsumerState state = ConsumerState.NEW;

		private volatile Runnable task;

		private volatile String shardIterator;

		private volatile long sleepUntil;

		ShardConsumer(KinesisShardOffset shardOffset) {
			this.shardOffset = new KinesisShardOffset(shardOffset);
			this.key = buildCheckpointKeyForShard(shardOffset.getStream(), shardOffset.getShard());
			this.checkpointer = new ShardCheckpointer(KinesisMessageDrivenChannelAdapter.this.checkpointStore, this.key);
		}

		void setNotifier(Runnable notifier) {
			this.notifier = notifier;
		}

		void stop() {
			this.state = ConsumerState.STOP;
			if (KinesisMessageDrivenChannelAdapter.this.lockRegistry != null) {
				KinesisMessageDrivenChannelAdapter.this.shardConsumerManager.unlock(this.key);
			}
			if (this.notifier != null) {
				this.notifier.run();
			}
		}

		void close() {
			stop();
			this.checkpointer.close();
		}

		void execute() {
			if (this.task == null) {
				switch (this.state) {

					case NEW:
					case EXPIRED:
						this.task = () -> {
							try {
								if (logger.isInfoEnabled() && this.state == ConsumerState.NEW) {
									logger.info("The [" + this + "] has been started.");
								}
								if (this.shardOffset.isReset()) {
									this.checkpointer.remove();
								}
								else {
									String checkpoint = this.checkpointer.getCheckpoint();
									if (checkpoint != null) {
										this.shardOffset.setSequenceNumber(checkpoint);
										this.shardOffset.setIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
									}
								}

								GetShardIteratorRequest shardIteratorRequest = this.shardOffset.toShardIteratorRequest();
								this.shardIterator =
										KinesisMessageDrivenChannelAdapter.this.amazonKinesis
												.getShardIterator(shardIteratorRequest)
												.getShardIterator();
								if (ConsumerState.STOP != this.state) {
									this.state = ConsumerState.CONSUME;
								}
							}
							finally {
								this.task = null;
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
										"] on the checkpoint [" + this.checkpointer.getCheckpoint() +
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
					if (this.notifier != null) {
						this.notifier.run();
					}
					if (KinesisMessageDrivenChannelAdapter.this.concurrency == 0) {
						KinesisMessageDrivenChannelAdapter.this.consumerExecutor.execute(this.task);
					}
				}
			}
		}

		private Runnable processTask() {
			return () -> {
				GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
				getRecordsRequest.setShardIterator(this.shardIterator);
				getRecordsRequest.setLimit(KinesisMessageDrivenChannelAdapter.this.recordsLimit);

				GetRecordsResult result = null;

				try {
					result = getRecords(getRecordsRequest);
					if (result != null) {
						List<Record> records = result.getRecords();

						if (!records.isEmpty()) {
							processRecords(records);
						}
					}
				}
				finally {
					attributesHolder.remove();
					if (result != null) {
						this.shardIterator = result.getNextShardIterator();

						if (this.shardIterator == null) {
							// Shard is closed: nothing to consume any more.
							// Resharding is possible.
							stop();
						}

						if (ConsumerState.STOP != this.state && result.getRecords().isEmpty()) {
							if (logger.isDebugEnabled()) {
								logger.debug("No records for [" + this +
										"] on sequenceNumber [" +
										this.checkpointer.getLastCheckpointValue() +
										"]. Suspend consuming for [" +
										KinesisMessageDrivenChannelAdapter.this.consumerBackoff + "] milliseconds.");
							}
							prepareSleepState();
						}
					}

					this.task = null;
				}
			};
		}

		private GetRecordsResult getRecords(GetRecordsRequest getRecordsRequest) {
			try {
				return KinesisMessageDrivenChannelAdapter.this.amazonKinesis.getRecords(getRecordsRequest);
			}
			catch (ExpiredIteratorException e) {
				// Iterator expired, but this does not mean that shard no longer contains records.
				// Lets acquire iterator again (using checkpointer for iterator start sequence number).
				if (logger.isInfoEnabled()) {
					logger.info("Shard iterator for [" + ShardConsumer.this + "] expired.\n" +
							"A new one will be started from the check pointed sequence number.");
				}
				this.state = ConsumerState.EXPIRED;
			}
			catch (ProvisionedThroughputExceededException e) {
				if (logger.isWarnEnabled()) {
					logger.warn("GetRecords request throttled for [" + ShardConsumer.this +
							"] with the reason: " + e.getErrorMessage());
				}
				// We are throttled, so let's sleep
				prepareSleepState();
			}

			return null;
		}

		private void prepareSleepState() {
			ShardConsumer.this.sleepUntil = System.currentTimeMillis() +
					KinesisMessageDrivenChannelAdapter.this.consumerBackoff;
			ShardConsumer.this.state = ConsumerState.SLEEP;
		}

		private void processRecords(List<Record> records) {
			if (logger.isTraceEnabled()) {
				logger.trace("Processing records: " + records + " for [" + ShardConsumer.this + "]");
			}

			this.checkpointer.setHighestSequence(records.get(records.size() - 1).getSequenceNumber());

			switch (KinesisMessageDrivenChannelAdapter.this.listenerMode) {
				case record:
					for (Record record : records) {
						performSend(prepareMessageForRecord(record), record);

						if (CheckpointMode.record.equals(KinesisMessageDrivenChannelAdapter.this.checkpointMode)) {
							this.checkpointer.checkpoint(record.getSequenceNumber());
						}
					}

					break;

				case batch:
					Object payload = records;

					if (KinesisMessageDrivenChannelAdapter.this.embeddedHeadersMapper != null) {
						payload = records.stream()
								.map(this::prepareMessageForRecord)
								.collect(Collectors.toList());
					}

					final List<String> partitionKeys;
					final List<String> sequenceNumbers;
					if (KinesisMessageDrivenChannelAdapter.this.converter != null) {
						partitionKeys = new ArrayList<>();
						sequenceNumbers = new ArrayList<>();

						payload = records.stream()
								.map(r -> {
									partitionKeys.add(r.getPartitionKey());
									sequenceNumbers.add(r.getSequenceNumber());

									return KinesisMessageDrivenChannelAdapter.this.converter
											.convert(r.getData().array());
								})
								.collect(Collectors.toList());
					}
					else {
						partitionKeys = null;
						sequenceNumbers = null;
					}

					AbstractIntegrationMessageBuilder<?> messageBuilder =
							getMessageBuilderFactory()
									.withPayload(payload)
									.setHeader(AwsHeaders.RECEIVED_PARTITION_KEY, partitionKeys)
									.setHeader(AwsHeaders.RECEIVED_SEQUENCE_NUMBER, sequenceNumbers);

					performSend(messageBuilder, records);

					break;
			}

			if (CheckpointMode.batch.equals(KinesisMessageDrivenChannelAdapter.this.checkpointMode)) {
				this.checkpointer.checkpoint();
			}
		}

		private AbstractIntegrationMessageBuilder<Object> prepareMessageForRecord(Record record) {
			Object payload = record.getData().array();
			Message<?> messageToUse = null;

			if (KinesisMessageDrivenChannelAdapter.this.embeddedHeadersMapper != null) {
				try {
					messageToUse =
							KinesisMessageDrivenChannelAdapter.this.embeddedHeadersMapper
									.toMessage((byte[]) payload);

					payload = messageToUse.getPayload();
				}
				catch (Exception e) {
					logger.warn("Could not parse embedded headers. Remain payload untouched.", e);
				}
			}

			if (payload instanceof byte[] &&
					KinesisMessageDrivenChannelAdapter.this.converter != null) {

				payload = KinesisMessageDrivenChannelAdapter.this.converter.convert((byte[]) payload);
			}

			AbstractIntegrationMessageBuilder<Object> messageBuilder =
					getMessageBuilderFactory()
							.withPayload(payload)
							.setHeader(AwsHeaders.RECEIVED_PARTITION_KEY, record.getPartitionKey())
							.setHeader(AwsHeaders.RECEIVED_SEQUENCE_NUMBER, record.getSequenceNumber());

			if (messageToUse != null) {
				messageBuilder.copyHeadersIfAbsent(messageToUse.getHeaders());
			}

			return messageBuilder;
		}

		private void performSend(AbstractIntegrationMessageBuilder<?> messageBuilder, Object rawRecord) {
			messageBuilder.setHeader(AwsHeaders.RECEIVED_STREAM, this.shardOffset.getStream())
					.setHeader(AwsHeaders.SHARD, this.shardOffset.getShard());

			if (CheckpointMode.manual.equals(KinesisMessageDrivenChannelAdapter.this.checkpointMode)) {
				messageBuilder.setHeader(AwsHeaders.CHECKPOINTER, this.checkpointer);
			}

			Message<?> messageToSend = messageBuilder.build();
			setAttributesIfNecessary(rawRecord, messageToSend);
			try {
				sendMessage(messageToSend);
			}
			catch (Exception e) {
				logger.info("Got an exception during sending a '" + messageToSend + "'" +
						"\nfor the '" + rawRecord + "'.\n" +
						"Consider to use 'errorChannel' flow for the compensation logic.", e);
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

		NEW, EXPIRED, CONSUME, SLEEP, STOP

	}

	private final class ConsumerInvoker implements SchedulingAwareRunnable {

		private final Queue<ShardConsumer> consumers = new ConcurrentLinkedQueue<>();

		private final Semaphore processBarrier = new Semaphore(0);

		private final Runnable notifier = this::notifyBarrier;

		ConsumerInvoker(Collection<ShardConsumer> shardConsumers) {
			for (ShardConsumer shardConsumer : shardConsumers) {
				addConsumer(shardConsumer);
			}
		}

		void addConsumer(ShardConsumer shardConsumer) {
			shardConsumer.setNotifier(this.notifier);
			this.consumers.add(shardConsumer);
		}

		void notifyBarrier() {
			this.processBarrier.release();
		}

		@Override
		public void run() {
			while (KinesisMessageDrivenChannelAdapter.this.active) {
				try {
					this.processBarrier.acquire();
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();

					throw new IllegalStateException("ConsumerInvoker thread [" + this + "] has been interrupted", e);
				}

				for (Iterator<ShardConsumer> iterator = this.consumers.iterator(); iterator.hasNext(); ) {
					ShardConsumer shardConsumer = iterator.next();
					if (ConsumerState.STOP == shardConsumer.state) {
						iterator.remove();
					}
					else {
						if (shardConsumer.task != null) {
							try {
								shardConsumer.task.run();
							}
							catch (Exception e) {
								logger.info("Got an exception " + e + " during [" + shardConsumer + "] task invocation.\n" +
										"Process will be retried on the next iteration.");
							}
						}
					}
				}
				synchronized (KinesisMessageDrivenChannelAdapter.this.consumerInvokers) {
					// The attempt to survive if ShardConsumer has been added during synchronization
					if (this.consumers.isEmpty()) {
						KinesisMessageDrivenChannelAdapter.this.consumerInvokers.remove(this);
						break;
					}
				}
			}
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

	}

	private final class ShardConsumerManager implements SchedulingAwareRunnable {

		private final Map<String, KinesisShardOffset> shardOffsetsToConsumer = new ConcurrentHashMap<>();

		private final Map<String, Lock> locks = new HashMap<>();

		private final Queue<String> forUnlocking = new ConcurrentLinkedQueue<>();

		ShardConsumerManager() {
		}

		void addShardToConsume(KinesisShardOffset kinesisShardOffset) {
			String lockKey = buildCheckpointKeyForShard(kinesisShardOffset.getStream(), kinesisShardOffset.getShard());
			this.shardOffsetsToConsumer.put(lockKey, kinesisShardOffset);
		}

		void unlock(String lockKey) {
			this.forUnlocking.add(lockKey);
		}

		@Override
		public void run() {
			try {
				while (!Thread.currentThread().isInterrupted()) {

					this.shardOffsetsToConsumer.entrySet()
							.removeIf(entry -> {
								boolean remove = true;
								if (KinesisMessageDrivenChannelAdapter.this.lockRegistry != null) {
									String key = entry.getKey();
									Lock lock =
											KinesisMessageDrivenChannelAdapter.this.lockRegistry.obtain(key);
									try {
										if (lock.tryLock()) {
											this.locks.put(key, lock);
										}
										else {
											remove = false;
										}

									}
									catch (Exception e) {
										logger.error("Error during locking: " + lock, e);
									}
								}

								if (remove) {
									populateConsumer(entry.getValue());
								}

								return remove;
							});

					while (KinesisMessageDrivenChannelAdapter.this.lockRegistry != null) {
						String lockKey = this.forUnlocking.poll();
						if (lockKey != null) {
							Lock lock = this.locks.remove(lockKey);
							if (lock != null) {
								try {
									lock.unlock();
								}
								catch (Exception e) {
									logger.error("Error during unlocking: " + lock, e);
								}
							}
						}
						else {
							break;
						}
					}

					try {
						Thread.sleep(250);
					}
					catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						throw new IllegalStateException("ShardConsumerManager Thread [" +
								this + "] has been interrupted", e);
					}
				}
			}
			finally {
				for (Iterator<Lock> iterator = this.locks.values().iterator(); iterator.hasNext(); ) {
					Lock lock = iterator.next();
					try {
						lock.unlock();
					}
					catch (Exception e) {
						logger.error("Error during unlocking: " + lock, e);
					}
					finally {
						iterator.remove();
					}
				}
			}
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

	}

}
