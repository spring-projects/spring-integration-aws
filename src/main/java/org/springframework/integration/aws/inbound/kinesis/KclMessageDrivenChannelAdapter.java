/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.integration.aws.inbound.kinesis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.springframework.core.AttributeAccessor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.serializer.support.DeserializingConverter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.core.task.support.ExecutorServiceAdapter;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.model.Record;

/**
 * The {@link MessageProducerSupport} implementation for receiving data from Amazon
 * Kinesis stream(s) using AWS KCL.
 *
 * @author Hervé Fortin
 * @author Artem Bilan
 * @author Dirk Bonhomme
 *
 * @since 2.2.0
 */
@ManagedResource
@IntegrationManagedResource
public class KclMessageDrivenChannelAdapter extends MessageProducerSupport {

	private static final ThreadLocal<AttributeAccessor> attributesHolder = new ThreadLocal<>();

	private final RecordProcessorFactory recordProcessorFactory = new RecordProcessorFactory();

	private final String stream;

	private final AmazonKinesis kinesisClient;

	private final AWSCredentialsProvider kinesisProxyCredentialsProvider;

	private final AmazonCloudWatch cloudWatchClient;

	private final AmazonDynamoDB dynamoDBClient;

	private TaskExecutor executor = new SimpleAsyncTaskExecutor();

	private String consumerGroup = "SpringIntegration";

	private InboundMessageMapper<byte[]> embeddedHeadersMapper;

	private KinesisClientLibConfiguration config;

	private InitialPositionInStream streamInitialSequence = InitialPositionInStream.LATEST;

	private int idleBetweenPolls = 1000;

	private int consumerBackoff = 1000;

	private Converter<byte[], Object> converter = new DeserializingConverter();

	private ListenerMode listenerMode = ListenerMode.record;

	private long checkpointsInterval = 5_000L;

	private CheckpointMode checkpointMode = CheckpointMode.batch;

	private String workerId = UUID.randomUUID().toString();

	private boolean bindSourceRecord;

	private volatile Worker scheduler;

	public KclMessageDrivenChannelAdapter(String streams) {
		this(streams, AmazonKinesisClientBuilder.defaultClient(), AmazonCloudWatchClientBuilder.defaultClient(),
				AmazonDynamoDBClientBuilder.defaultClient(), new DefaultAWSCredentialsProviderChain());
	}

	public KclMessageDrivenChannelAdapter(String streams, Regions region) {
		this(streams, AmazonKinesisClient.builder().withRegion(region).build(),
				AmazonCloudWatchClient.builder().withRegion(region).build(),
				AmazonDynamoDBClient.builder().withRegion(region).build(), new DefaultAWSCredentialsProviderChain());
	}

	public KclMessageDrivenChannelAdapter(String stream, AmazonKinesis kinesisClient, AmazonCloudWatch cloudWatchClient,
			AmazonDynamoDB dynamoDBClient, AWSCredentialsProvider kinesisProxyCredentialsProvider) {

		Assert.notNull(stream, "'stream' must not be null.");
		Assert.notNull(kinesisClient, "'kinesisClient' must not be null.");
		Assert.notNull(cloudWatchClient, "'cloudWatchClient' must not be null.");
		Assert.notNull(dynamoDBClient, "'dynamoDBClient' must not be null.");
		Assert.notNull(kinesisProxyCredentialsProvider, "'kinesisProxyCredentialsProvider' must not be null.");
		this.stream = stream;
		this.kinesisClient = kinesisClient;
		this.cloudWatchClient = cloudWatchClient;
		this.dynamoDBClient = dynamoDBClient;
		this.kinesisProxyCredentialsProvider = kinesisProxyCredentialsProvider;
	}

	public void setExecutor(TaskExecutor executor) {
		Assert.notNull(executor, "'executor' must not be null.");
		this.executor = executor;
	}

	public void setConsumerGroup(String consumerGroup) {
		Assert.hasText(consumerGroup, "'consumerGroup' must not be empty");
		this.consumerGroup = consumerGroup;
	}

	/**
	 * Specify an {@link InboundMessageMapper} to extract message headers embedded into
	 * the record data.
	 * @param embeddedHeadersMapper the {@link InboundMessageMapper} to use.
	 */
	public void setEmbeddedHeadersMapper(InboundMessageMapper<byte[]> embeddedHeadersMapper) {
		this.embeddedHeadersMapper = embeddedHeadersMapper;
	}

	public void setStreamInitialSequence(InitialPositionInStream streamInitialSequence) {
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

	/**
	 * Sets the interval between 2 checkpoints.
	 * @param checkpointsInterval interval between 2 checkpoints (in milliseconds)
	 */
	public void setCheckpointsInterval(long checkpointsInterval) {
		this.checkpointsInterval = checkpointsInterval;
	}

	public void setCheckpointMode(CheckpointMode checkpointMode) {
		Assert.notNull(checkpointMode, "'checkpointMode' must not be null");
		this.checkpointMode = checkpointMode;
	}

	/**
	 * Sets the worker identifier used to distinguish different workers/processes of a
	 * Kinesis application.
	 * @param workerId the worker identifier to use
	 */
	public void setWorkerId(String workerId) {
		Assert.hasText(workerId, "'workerId' must not be null or empty");
		this.workerId = workerId;
	}

	/**
	 * Set to true to bind the source consumer record in the header named
	 * {@link IntegrationMessageHeaderAccessor#SOURCE_DATA}. Does not apply to batch
	 * listeners.
	 * @param bindSourceRecord true to bind.
	 * @since 2.2
	 */
	public void setBindSourceRecord(boolean bindSourceRecord) {
		this.bindSourceRecord = bindSourceRecord;
	}

	@Override
	protected void onInit() {
		super.onInit();

		this.config =
				new KinesisClientLibConfiguration(this.consumerGroup,
						this.stream,
						null,
						this.streamInitialSequence,
						this.kinesisProxyCredentialsProvider,
						null,
						null,
						KinesisClientLibConfiguration.DEFAULT_FAILOVER_TIME_MILLIS,
						this.workerId,
						KinesisClientLibConfiguration.DEFAULT_MAX_RECORDS,
						this.idleBetweenPolls,
						false,
						KinesisClientLibConfiguration.DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS,
						KinesisClientLibConfiguration.DEFAULT_SHARD_SYNC_INTERVAL_MILLIS,
						KinesisClientLibConfiguration.DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION,
						new ClientConfiguration(),
						new ClientConfiguration(),
						new ClientConfiguration(),
						this.consumerBackoff,
						KinesisClientLibConfiguration.DEFAULT_METRICS_BUFFER_TIME_MILLIS,
						KinesisClientLibConfiguration.DEFAULT_METRICS_MAX_QUEUE_SIZE,
						KinesisClientLibConfiguration.DEFAULT_VALIDATE_SEQUENCE_NUMBER_BEFORE_CHECKPOINTING,
						null,
						KinesisClientLibConfiguration.DEFAULT_SHUTDOWN_GRACE_MILLIS);
	}

	@Override
	protected void doStart() {
		super.doStart();

		if (ListenerMode.batch.equals(this.listenerMode) && CheckpointMode.record.equals(this.checkpointMode)) {
			this.checkpointMode = CheckpointMode.batch;
			logger.warn("The 'checkpointMode' is overridden from [CheckpointMode.record] to [CheckpointMode.batch] "
					+ "because it does not make sense in case of [ListenerMode.batch].");
		}

		this.scheduler =
				new Worker
						.Builder()
						.kinesisClient(this.kinesisClient)
						.dynamoDBClient(this.dynamoDBClient)
						.cloudWatchClient(this.cloudWatchClient)
						.recordProcessorFactory(this.recordProcessorFactory)
						.execService(new ExecutorServiceAdapter(this.executor))
						.config(this.config)
						.build();

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
	public void destroy() {
		super.destroy();
		if (isRunning()) {
			this.scheduler.shutdown();
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
		return "KclMessageDrivenChannelAdapter{consumerGroup='" + this.consumerGroup + '\'' + ", stream='" + this.stream
				+ "'}";
	}

	private class RecordProcessorFactory implements IRecordProcessorFactory {

		@Override
		public IRecordProcessor createProcessor() {
			return new RecordProcessor();
		}

	}

	/**
	 * Processes records and checkpoints progress.
	 */
	private class RecordProcessor implements IRecordProcessor {

		private String shardId;

		private long nextCheckpointTimeInMillis;

		@Override
		public void initialize(String shardId) {
			this.shardId = shardId;
			if (logger.isInfoEnabled()) {
				logger.info("Initializing record processor for shard: " + this.shardId);
			}
		}

		@Override
		public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
			if (logger.isDebugEnabled()) {
				logger.debug("Processing " + records.size() + " records from " + this.shardId);
			}

			try {
				if (ListenerMode.record.equals(KclMessageDrivenChannelAdapter.this.listenerMode)) {
					for (Record record : records) {
						processSingleRecord(record, checkpointer);
						checkpointIfRecordMode(checkpointer, record);
						checkpointIfPeriodicMode(checkpointer, record);
					}
				}
				else if (ListenerMode.batch.equals(KclMessageDrivenChannelAdapter.this.listenerMode)) {
					processMultipleRecords(records, checkpointer);
					checkpointIfPeriodicMode(checkpointer, null);
				}
				checkpointIfBatchMode(checkpointer);
			}
			finally {
				attributesHolder.remove();
			}
		}

		private void processSingleRecord(Record record, IRecordProcessorCheckpointer checkpointer) {
			performSend(prepareMessageForRecord(record), record, checkpointer);
		}

		private void processMultipleRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
			AbstractIntegrationMessageBuilder<?> messageBuilder = getMessageBuilderFactory().withPayload(records);
			if (KclMessageDrivenChannelAdapter.this.embeddedHeadersMapper != null) {
				List<Message<Object>> payload =
						records.stream()
								.map(this::prepareMessageForRecord)
								.map(AbstractIntegrationMessageBuilder::build)
								.collect(Collectors.toList());

				messageBuilder = getMessageBuilderFactory().withPayload(payload);
			}
			else if (KclMessageDrivenChannelAdapter.this.converter != null) {
				final List<String> partitionKeys = new ArrayList<>();
				final List<String> sequenceNumbers = new ArrayList<>();

				List<Object> payload = records.stream()
						.map(r -> {
							partitionKeys.add(r.getPartitionKey());
							sequenceNumbers.add(r.getSequenceNumber());

							return KclMessageDrivenChannelAdapter.this.converter.convert(r.getData().array());
						})
						.collect(Collectors.toList());

				messageBuilder = getMessageBuilderFactory().withPayload(payload)
						.setHeader(AwsHeaders.RECEIVED_PARTITION_KEY, partitionKeys)
						.setHeader(AwsHeaders.RECEIVED_SEQUENCE_NUMBER, sequenceNumbers);
			}

			performSend(messageBuilder, records, checkpointer);
		}

		private AbstractIntegrationMessageBuilder<Object> prepareMessageForRecord(Record record) {
			Object payload = record.getData().array();
			Message<?> messageToUse = null;

			if (KclMessageDrivenChannelAdapter.this.embeddedHeadersMapper != null) {
				try {
					messageToUse = KclMessageDrivenChannelAdapter.this.embeddedHeadersMapper
							.toMessage((byte[]) payload);
					if (messageToUse == null) {
						throw new IllegalStateException("The 'embeddedHeadersMapper' returned null for payload: "
								+ Arrays.toString((byte[]) payload));
					}
					payload = messageToUse.getPayload();
				}
				catch (Exception e) {
					logger.warn("Could not parse embedded headers. Remain payload untouched.", e);
				}
			}

			if (payload instanceof byte[] && KclMessageDrivenChannelAdapter.this.converter != null) {
				payload = KclMessageDrivenChannelAdapter.this.converter.convert((byte[]) payload);
			}

			AbstractIntegrationMessageBuilder<Object> messageBuilder = getMessageBuilderFactory().withPayload(payload)
					.setHeader(AwsHeaders.RECEIVED_PARTITION_KEY, record.getPartitionKey())
					.setHeader(AwsHeaders.RECEIVED_SEQUENCE_NUMBER, record.getSequenceNumber());

			if (KclMessageDrivenChannelAdapter.this.bindSourceRecord) {
				messageBuilder.setHeader(IntegrationMessageHeaderAccessor.SOURCE_DATA, record);
			}

			if (messageToUse != null) {
				messageBuilder.copyHeadersIfAbsent(messageToUse.getHeaders());
			}

			return messageBuilder;
		}

		private void performSend(AbstractIntegrationMessageBuilder<?> messageBuilder, Object rawRecord,
				IRecordProcessorCheckpointer checkpointer) {
			messageBuilder.setHeader(AwsHeaders.RECEIVED_STREAM, KclMessageDrivenChannelAdapter.this.stream)
					.setHeader(AwsHeaders.SHARD, this.shardId);

			if (CheckpointMode.manual.equals(KclMessageDrivenChannelAdapter.this.checkpointMode)) {
				messageBuilder.setHeader(AwsHeaders.CHECKPOINTER, checkpointer);
			}

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
		 * If there's an error channel, we create a new attributes holder here. Then set
		 * the attributes for use by the {@link ErrorMessageStrategy}.
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

		/**
		 * Checkpoint with retries.
		 * @param checkpointer checkpointer
		 * @param record last processed record
		 */
		private void checkpoint(IRecordProcessorCheckpointer checkpointer, @Nullable Record record) {
			if (logger.isInfoEnabled()) {
				logger.info("Checkpointing shard " + shardId);
			}
			try {
				if (record == null) {
					checkpointer.checkpoint();
				}
				else {
					checkpointer.checkpoint(record);
				}
			}
			catch (ShutdownException se) {
				// Ignore checkpoint if the processor instance has been shutdown (fail
				// over).
				logger.info("Caught shutdown exception, skipping checkpoint.", se);
			}
			catch (ThrottlingException e) {
				if (logger.isInfoEnabled()) {
					logger.info("Transient issue when checkpointing", e);
				}
			}
			catch (InvalidStateException e) {
				// This indicates an issue with the DynamoDB table (check for table,
				// provisioned
				// IOPS).
				logger.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.",
						e);
			}
		}

		private void checkpointIfBatchMode(IRecordProcessorCheckpointer checkpointer) {
			if (CheckpointMode.batch.equals(KclMessageDrivenChannelAdapter.this.checkpointMode)) {
				checkpoint(checkpointer, null);
			}
		}

		private void checkpointIfRecordMode(IRecordProcessorCheckpointer checkpointer, Record record) {
			if (CheckpointMode.record.equals(KclMessageDrivenChannelAdapter.this.checkpointMode)) {
				checkpoint(checkpointer, record);
			}
		}

		private void checkpointIfPeriodicMode(IRecordProcessorCheckpointer checkpointer, @Nullable Record record) {
			if (CheckpointMode.periodic.equals(KclMessageDrivenChannelAdapter.this.checkpointMode)
					&& System.currentTimeMillis() > nextCheckpointTimeInMillis) {
				checkpoint(checkpointer, record);
				this.nextCheckpointTimeInMillis = System.currentTimeMillis() + checkpointsInterval;
			}
		}

		@Override
		public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
			if (logger.isInfoEnabled()) {
				logger.info("Scheduler is shutting down for reason '" + reason + "'; checkpointing...");
			}
			try {
				checkpointer.checkpoint();
			}
			catch (ShutdownException | InvalidStateException e) {
				logger.error("Exception while checkpointing at requested shutdown. Giving up", e);
			}
		}

	}

}
