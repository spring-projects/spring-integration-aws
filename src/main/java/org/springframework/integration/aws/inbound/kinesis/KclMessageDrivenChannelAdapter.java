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

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.mapping.InboundMessageMapper;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.integration.support.management.IntegrationManagedResource;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

/**
 * The {@link MessageProducerSupport} implementation for receiving data from Amazon
 * Kinesis stream(s) using AWS KCL.
 *
 * @author Hervé Fortin
 *
 * @since 2.1.0
 */
@ManagedResource
@IntegrationManagedResource
public class KclMessageDrivenChannelAdapter extends MessageProducerSupport implements DisposableBean {

	private final String stream;

	private final Set<KinesisShardOffset> shardOffsets = new HashSet<>();

	private String consumerGroup = "SpringIntegration";

	private InboundMessageMapper<byte[]> embeddedHeadersMapper;

	private Scheduler scheduler;

	private String region;

	private InitialPositionInStreamExtended streamInitialSequence =
			InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);

	private int idleBetweenPolls;

	private int consumerBackoff;

	public KclMessageDrivenChannelAdapter(String streams) {
		Assert.notNull(streams, "'streams' must not be null.");
		this.stream = streams;
	}

	public void setConsumerGroup(String consumerGroup) {
		Assert.hasText(consumerGroup, "'consumerGroup' must not be empty");
		this.consumerGroup = consumerGroup;
	}

	/**
	 * Specify an {@link InboundMessageMapper} to extract message headers embedded
	 * into the record data.
	 *
	 * @param embeddedHeadersMapper the {@link InboundMessageMapper} to use.
	 * @since 2.0
	 */
	public void setEmbeddedHeadersMapper(InboundMessageMapper<byte[]> embeddedHeadersMapper) {
		this.embeddedHeadersMapper = embeddedHeadersMapper;
	}

	/**
	 * Takes no action by default. Subclasses may override this if they need
	 * lifecycle-managed behavior. Protected by 'lifecycleLock'.
	 */
	@Override
	protected void doStart() {
		super.doStart();

		String workerId = UUID.randomUUID().toString();
		RecordProcessorFactory recordProcessorFactory = new RecordProcessorFactory();

		Region parsedRegion = (this.region == null) ? null : Region.of(this.region);
		KinesisAsyncClient kinesisClient = KinesisAsyncClient.builder().region(parsedRegion).build();
		CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(parsedRegion).build();
		DynamoDbAsyncClient dynamoDBClient = DynamoDbAsyncClient.builder().region(parsedRegion).build();
		ConfigsBuilder configsBuilder = new ConfigsBuilder(this.stream, this.consumerGroup, kinesisClient, dynamoDBClient, cloudWatchClient, workerId, recordProcessorFactory);
		configsBuilder.retrievalConfig().initialPositionInStreamExtended(this.streamInitialSequence);
		configsBuilder.retrievalConfig().listShardsBackoffTimeInMillis(this.consumerBackoff);
		configsBuilder.coordinatorConfig().parentShardPollIntervalMillis(this.idleBetweenPolls);

		this.scheduler = new Scheduler(configsBuilder.checkpointConfig(),
			configsBuilder.coordinatorConfig(),
			configsBuilder.leaseManagementConfig(),
			configsBuilder.lifecycleConfig(),
			configsBuilder.metricsConfig(),
			configsBuilder.processorConfig(),
			configsBuilder.retrievalConfig());

		Thread schedulerThread = new Thread(() -> this.scheduler.run());
		schedulerThread.setDaemon(true);
		schedulerThread.start();
	}

	/**
	 * Takes no action by default. Subclasses may override this if they need
	 * lifecycle-managed behavior.
	 */
	@Override
	protected void doStop() {

		super.doStop();
		this.scheduler.shutdown();

	}

	private AbstractIntegrationMessageBuilder<Object> prepareMessageForRecord(KinesisClientRecord record) {
		ByteBuffer data = record.data();
		byte[] dataArray = new byte[data.remaining()];
		Object payload = dataArray;
		data.get(dataArray);
		Message<?> messageToUse = null;

		if (KclMessageDrivenChannelAdapter.this.embeddedHeadersMapper != null) {
			try {
				messageToUse = KclMessageDrivenChannelAdapter.this.embeddedHeadersMapper.toMessage((byte[]) payload);

				payload = messageToUse.getPayload();
			}
			catch (Exception e) {
				logger.warn("Could not parse embedded headers. Remain payload untouched.", e);
			}
		}

		AbstractIntegrationMessageBuilder<Object> messageBuilder = getMessageBuilderFactory().withPayload(payload)
				.setHeader(AwsHeaders.RECEIVED_PARTITION_KEY, record.partitionKey())
				.setHeader(AwsHeaders.RECEIVED_SEQUENCE_NUMBER, record.sequenceNumber());

		if (messageToUse != null) {
			messageBuilder.copyHeadersIfAbsent(messageToUse.getHeaders());
		}

		return messageBuilder;
	}

	private void performSend(AbstractIntegrationMessageBuilder<?> messageBuilder, Object rawRecord) {

		Message<?> messageToSend = messageBuilder.build();
		try {
			sendMessage(messageToSend);
		}
		catch (Exception e) {
			logger.error("Got an exception during sending a '" + messageToSend + "'" + "\nfor the '" + rawRecord
					+ "'.\n" + "Consider to use 'errorChannel' flow for the compensation logic.", e);
		}
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public void setStreamInitialSequence(InitialPositionInStream streamInitialSequence) {
		setStreamInitialSequence(InitialPositionInStreamExtended.newInitialPosition(streamInitialSequence));
	}

	public void setStreamInitialSequence(InitialPositionInStreamExtended streamInitialSequence) {
		Assert.notNull(streamInitialSequence, "'streamInitialSequence' must not be null");
		this.streamInitialSequence = streamInitialSequence;
	}

	public void setIdleBetweenPolls(int idleBetweenPolls) {
		Assert.notNull(idleBetweenPolls, "'idleBetweenPolls' must not be null");
		this.idleBetweenPolls = Math.max(250, idleBetweenPolls);
	}

	public void setConsumerBackoff(int consumerBackoff) {
		Assert.notNull(consumerBackoff, "'consumerBackoff' must not be null");
		this.consumerBackoff = Math.max(1000, consumerBackoff);
	}

	@Override
	public String toString() {
		return "KclMessageDrivenChannelAdapter{" + "shardOffsets=" + this.shardOffsets + ", consumerGroup='"
				+ this.consumerGroup + '\'' + '}';
	}

	private class RecordProcessorFactory implements ShardRecordProcessorFactory {
		@Override
		public ShardRecordProcessor shardRecordProcessor() {
			return new RecordProcessor();
		}
	}

	/**
	 * Processes records and checkpoints progress.
	 */
	private class RecordProcessor implements ShardRecordProcessor {

		private String shardId;

		private final Log logger = LogFactory.getLog(RecordProcessor.class);

		// Backoff and retry settings
		private static final int NUM_RETRIES = 10;

		// Checkpoint about once a minute
		private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
		private long nextCheckpointTimeInMillis;

		/** {@inheritDoc} */
		@Override
		public void initialize(InitializationInput initializationInput) {
			this.shardId = initializationInput.shardId();
			this.logger.info("Initializing record processor for shard: " + this.shardId);
		}

		/** {@inheritDoc} */
		@Override
		public void leaseLost(LeaseLostInput leaseLostInput) {
			this.logger.info("Lost lease, so terminating.");
		}

		/** {@inheritDoc} */
		@Override
		public void shardEnded(ShardEndedInput shardEndedInput) {
			try {
				this.logger.info("Reached shard end checkpointing.");
				shardEndedInput.checkpointer().checkpoint();
			}
			catch (ShutdownException | InvalidStateException e) {
				this.logger.error("Exception while checkpointing at shard end.  Giving up", e);
			}
		}

		/** {@inheritDoc} */
		@Override
		public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
			try {
				this.logger.info("Scheduler is shutting down, checkpointing.");
				shutdownRequestedInput.checkpointer().checkpoint();
			}
			catch (ShutdownException | InvalidStateException e) {
				this.logger.error("Exception while checkpointing at requested shutdown.  Giving up", e);
			}
		}

		/**
		 * Process records performing retries as needed. Skip "poison pill" records.
		 *
		 * @param records Data records to be processed.
		 */
		private void processRecordsWithRetries(List<KinesisClientRecord> records) {
			for (KinesisClientRecord record : records) {
				boolean processedSuccessfully = false;
				for (int i = 0; i < NUM_RETRIES; i++) {
					try {
						processSingleRecord(record);

						processedSuccessfully = true;
						break;
					}
					catch (Throwable t) {
						this.logger.warn("Caught throwable while processing record " + record, t);
					}

					// backoff if we encounter an exception.
					try {
						Thread.sleep(KclMessageDrivenChannelAdapter.this.consumerBackoff);
					}
					catch (InterruptedException e) {
						this.logger.debug("Interrupted sleep", e);
					}
				}

				if (!processedSuccessfully) {
					this.logger.error("Couldn't process record " + record + ". Skipping the record.");
				}
			}
		}

		/**
		 * Process a single record.
		 *
		 * @param record The record to be processed.
		 */
		private void processSingleRecord(KinesisClientRecord record) {

			// Convert AWS Record in Spring Message.
			performSend(prepareMessageForRecord(record), record);
		}

		/**
		 * Checkpoint with retries.
		 *
		 * @param checkpointer checkpointer
		 */
		private void checkpoint(RecordProcessorCheckpointer checkpointer) {
			this.logger.info("Checkpointing shard " + shardId);
			for (int i = 0; i < NUM_RETRIES; i++) {
				try {
					checkpointer.checkpoint();
					break;
				}
				catch (ShutdownException se) {
					// Ignore checkpoint if the processor instance has been shutdown (fail over).
					this.logger.info("Caught shutdown exception, skipping checkpoint.", se);
					break;
				}
				catch (ThrottlingException e) {
					// Backoff and re-attempt checkpoint upon transient failures
					if (i >= (NUM_RETRIES - 1)) {
						this.logger.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
						break;
					}
					else {
						this.logger.info("Transient issue when checkpointing - attempt " + (i + 1) + " of " + NUM_RETRIES, e);
					}
				}
				catch (InvalidStateException e) {
					// This indicates an issue with the DynamoDB table (check for table, provisioned
					// IOPS).
					this.logger.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.",
							e);
					break;
				}
				try {
					Thread.sleep(KclMessageDrivenChannelAdapter.this.consumerBackoff);
				}
				catch (InterruptedException e) {
					this.logger.debug("Interrupted sleep", e);
				}
			}
		}

		@Override
		public void processRecords(ProcessRecordsInput processRecordsInput) {
			List<KinesisClientRecord> records = processRecordsInput.records();
			this.logger.debug("Processing " + records.size() + " records from " + this.shardId);

			// Process records and perform all exception handling.
			processRecordsWithRetries(records);

			// Checkpoint once every checkpoint interval.
			if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
				checkpoint(processRecordsInput.checkpointer());
				this.nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
			}
		}
	}
}
