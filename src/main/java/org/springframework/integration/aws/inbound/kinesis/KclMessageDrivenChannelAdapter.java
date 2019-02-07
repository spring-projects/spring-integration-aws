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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.AttributeAccessor;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.mapping.InboundMessageMapper;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageUtils;
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

	private static final ThreadLocal<AttributeAccessor> attributesHolder = new ThreadLocal<>();

	private final String stream;

	private String consumerGroup = "SpringIntegration";

	private InboundMessageMapper<byte[]> embeddedHeadersMapper;

	private Scheduler scheduler;

	private final Executor executor;

	private final KinesisAsyncClient kinesisClient;

	private final CloudWatchAsyncClient cloudWatchClient;

	private final DynamoDbAsyncClient dynamoDBClient;

	private InitialPositionInStreamExtended streamInitialSequence =
			InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);

	private int idleBetweenPolls;

	private int consumerBackoff;

	private long checkpointsInterval = 60_000L;

	public KclMessageDrivenChannelAdapter(String streams, Executor executor) {
		this(streams, executor, KinesisAsyncClient.builder().build(),
				CloudWatchAsyncClient.builder().build(), DynamoDbAsyncClient.builder().build());
	}

	public KclMessageDrivenChannelAdapter(String streams, Executor executor, Region region) {
		this(streams, executor, KinesisAsyncClient.builder().region(region).build(),
				CloudWatchAsyncClient.builder().region(region).build(), DynamoDbAsyncClient.builder().region(region).build());
	}

	public KclMessageDrivenChannelAdapter(String stream, Executor executor,
			KinesisAsyncClient kinesisClient, CloudWatchAsyncClient cloudWatchClient, DynamoDbAsyncClient dynamoDBClient) {
		Assert.notNull(stream, "'stream' must not be null.");
		Assert.notNull(executor, "'executor' must not be null.");
		Assert.notNull(kinesisClient, "'kinesisClient' must not be null.");
		Assert.notNull(cloudWatchClient, "'cloudWatchClient' must not be null.");
		Assert.notNull(dynamoDBClient, "'dynamoDBClient' must not be null.");
		this.stream = stream;
		this.executor = executor;
		this.kinesisClient = kinesisClient;
		this.cloudWatchClient = cloudWatchClient;
		this.dynamoDBClient = dynamoDBClient;
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

	@Override
	protected void onInit() {
		super.onInit();

		String workerId = UUID.randomUUID().toString();
		RecordProcessorFactory recordProcessorFactory = new RecordProcessorFactory();

		ConfigsBuilder configsBuilder = new ConfigsBuilder(this.stream, this.consumerGroup,
				this.kinesisClient, this.dynamoDBClient, this.cloudWatchClient, workerId, recordProcessorFactory);
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
	}

	@Override
	protected void doStart() {
		super.doStart();
		this.executor.execute(this.scheduler);
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

	public void setStreamInitialSequence(InitialPositionInStream streamInitialSequence) {
		setStreamInitialSequenceExtended(InitialPositionInStreamExtended.newInitialPosition(streamInitialSequence));
	}

	public void setStreamInitialSequenceExtended(InitialPositionInStreamExtended streamInitialSequence) {
		Assert.notNull(streamInitialSequence, "'streamInitialSequence' must not be null");
		this.streamInitialSequence = streamInitialSequence;
	}

	public void setIdleBetweenPolls(int idleBetweenPolls) {
		this.idleBetweenPolls = Math.max(250, idleBetweenPolls);
	}

	public void setConsumerBackoff(int consumerBackoff) {
		this.consumerBackoff = Math.max(1000, consumerBackoff);
	}

	/**
	 * Sets the interval between 2 checkpoints.
	 *
	 * @param checkpointsInterval interval between 2 checkpoints (in milliseconds)
	 */
	public void setCheckpointsInterval(long checkpointsInterval) {
		this.checkpointsInterval = checkpointsInterval;
	}

	@Override
	public String toString() {
		return "KclMessageDrivenChannelAdapter{consumerGroup='" + this.consumerGroup + '\'' + ", stream='" + this.stream + "'}";
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

		private long nextCheckpointTimeInMillis;

		/** {@inheritDoc} */
		@Override
		public void initialize(InitializationInput initializationInput) {
			this.shardId = initializationInput.shardId();
			if (logger.isInfoEnabled()) {
				logger.info("Initializing record processor for shard: " + this.shardId);
			}
		}

		/** {@inheritDoc} */
		@Override
		public void leaseLost(LeaseLostInput leaseLostInput) {
			logger.info("Lost lease, so terminating.");
		}

		/** {@inheritDoc} */
		@Override
		public void shardEnded(ShardEndedInput shardEndedInput) {
			try {
				logger.info("Reached shard end checkpointing.");
				shardEndedInput.checkpointer().checkpoint();
			}
			catch (ShutdownException | InvalidStateException e) {
				logger.error("Exception while checkpointing at shard end.  Giving up", e);
			}
		}

		/** {@inheritDoc} */
		@Override
		public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
			try {
				logger.info("Scheduler is shutting down, checkpointing.");
				shutdownRequestedInput.checkpointer().checkpoint();
			}
			catch (ShutdownException | InvalidStateException e) {
				logger.error("Exception while checkpointing at requested shutdown.  Giving up", e);
			}
		}

		/**
		 * Process records. Skip "poison pill" records.
		 *
		 * @param records Data records to be processed.
		 */
		private void processRecords(List<KinesisClientRecord> records) {
			for (KinesisClientRecord record : records) {
				try {
					processSingleRecord(record);
				}
				catch (Throwable t) {
					logger.warn("Caught throwable while processing record " + record, t);
				}
				finally {
					attributesHolder.remove();
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

		private void performSend(AbstractIntegrationMessageBuilder<?> messageBuilder, Object rawRecord) {
			Message<?> messageToSend = messageBuilder.build();
			setAttributesIfNecessary(rawRecord, messageToSend);
			try {
				sendMessage(messageToSend);
			}
			catch (Exception e) {
				logger.error("Got an exception during sending a '" + messageToSend + "'" + "\nfor the '" + rawRecord
						+ "'.\n" + "Consider to use 'errorChannel' flow for the compensation logic.", e);
			}
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
					.setHeader(AwsHeaders.RECEIVED_SEQUENCE_NUMBER, record.sequenceNumber())
					.setHeader(AwsHeaders.RECEIVED_STREAM, KclMessageDrivenChannelAdapter.this.stream)
					.setHeader(AwsHeaders.SHARD, this.shardId);

			if (messageToUse != null) {
				messageBuilder.copyHeadersIfAbsent(messageToUse.getHeaders());
			}

			return messageBuilder;
		}

		/**
		 * Checkpoint with retries.
		 *
		 * @param checkpointer checkpointer
		 */
		private void checkpoint(RecordProcessorCheckpointer checkpointer) {
			if (logger.isInfoEnabled()) {
				logger.info("Checkpointing shard " + shardId);
			}
			try {
				checkpointer.checkpoint();
			}
			catch (ShutdownException se) {
				// Ignore checkpoint if the processor instance has been shutdown (fail over).
				logger.info("Caught shutdown exception, skipping checkpoint.", se);
			}
			catch (ThrottlingException e) {
				if (logger.isInfoEnabled()) {
					logger.info("Transient issue when checkpointing", e);
				}
			}
			catch (InvalidStateException e) {
				// This indicates an issue with the DynamoDB table (check for table, provisioned
				// IOPS).
				logger.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.",
						e);
			}
		}

		@Override
		public void processRecords(ProcessRecordsInput processRecordsInput) {
			List<KinesisClientRecord> records = processRecordsInput.records();
			if (logger.isDebugEnabled()) {
				logger.debug("Processing " + records.size() + " records from " + this.shardId);
			}

			// Process records and perform all exception handling.
			processRecords(records);

			// Checkpoint once every checkpoint interval.
			if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
				checkpoint(processRecordsInput.checkpointer());
				this.nextCheckpointTimeInMillis = System.currentTimeMillis() + checkpointsInterval;
			}
		}
	}
}
