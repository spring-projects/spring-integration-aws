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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.mapping.InboundMessageMapper;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.integration.support.management.IntegrationManagedResource;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;

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

	private final AWSCredentialsProvider awsCredentialsProvider;

	private final String stream;

	private final Set<KinesisShardOffset> shardOffsets = new HashSet<>();

	private String consumerGroup = "SpringIntegration";

	private InboundMessageMapper<byte[]> embeddedHeadersMapper;

	private Worker worker;

	private String region;

	private InitialPositionInStream streamInitialSequence = InitialPositionInStream.LATEST;

	private int idleBetweenPolls;

	private int consumerBackoff;

	public KclMessageDrivenChannelAdapter(AWSCredentialsProvider awsCredentialsProvider, String streams) {
		Assert.notNull(awsCredentialsProvider, "'awsCredentialsProvider' must not be null.");
		Assert.notNull(streams, "'streams' must not be null.");
		this.awsCredentialsProvider = awsCredentialsProvider;
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
		KinesisClientLibConfiguration kinesisClientLibConfiguration = new KinesisClientLibConfiguration(
				this.consumerGroup, this.stream, this.awsCredentialsProvider, workerId);

		kinesisClientLibConfiguration.withInitialPositionInStream(this.streamInitialSequence);
		kinesisClientLibConfiguration.withRegionName(this.region);

		kinesisClientLibConfiguration.withListShardsBackoffTimeInMillis(this.consumerBackoff);
		kinesisClientLibConfiguration.withParentShardPollIntervalMillis(this.idleBetweenPolls);

		IRecordProcessorFactory recordProcessorFactory = new RecordProcessorFactory();

		this.worker = new Worker.Builder()
				.recordProcessorFactory(recordProcessorFactory)
				.config(kinesisClientLibConfiguration)
				.build();

		try {
			new Thread(() -> this.worker.run()).start();
		}
		catch (Throwable t) {
			logger.error("Caught throwable while processing data.", t);
		}
	}

	/**
	 * Takes no action by default. Subclasses may override this if they need
	 * lifecycle-managed behavior.
	 */
	@Override
	protected void doStop() {

		super.doStop();
		this.worker.shutdown();

	}

	private AbstractIntegrationMessageBuilder<Object> prepareMessageForRecord(Record record) {
		Object payload = record.getData().array();
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
				.setHeader(AwsHeaders.RECEIVED_PARTITION_KEY, record.getPartitionKey())
				.setHeader(AwsHeaders.RECEIVED_SEQUENCE_NUMBER, record.getSequenceNumber());

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

	private class RecordProcessorFactory implements IRecordProcessorFactory {
		/**
		 * {@inheritDoc}
		 */
		@Override
		public IRecordProcessor createProcessor() {
			return new RecordProcessor();
		}
	}

	/**
	 * Processes records and checkpoints progress.
	 */
	private class RecordProcessor implements IRecordProcessor {

		private String kinesisShardId;

		// Backoff and retry settings
		private static final int NUM_RETRIES = 10;

		// Checkpoint about once a minute
		private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
		private long nextCheckpointTimeInMillis;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void initialize(InitializationInput initializationInput) {
			this.kinesisShardId = initializationInput.getShardId();
			logger.info("Initializing record processor for shard: " + this.kinesisShardId);
		}

		/**
		 * Process records performing retries as needed. Skip "poison pill" records.
		 *
		 * @param records Data records to be processed.
		 */
		private void processRecordsWithRetries(List<Record> records) {
			for (Record record : records) {
				boolean processedSuccessfully = false;
				for (int i = 0; i < NUM_RETRIES; i++) {
					try {
						processSingleRecord(record);

						processedSuccessfully = true;
						break;
					}
					catch (Throwable t) {
						logger.warn("Caught throwable while processing record " + record, t);
					}

					// backoff if we encounter an exception.
					try {
						Thread.sleep(KclMessageDrivenChannelAdapter.this.consumerBackoff);
					}
					catch (InterruptedException e) {
						logger.debug("Interrupted sleep", e);
					}
				}

				if (!processedSuccessfully) {
					logger.error("Couldn't process record " + record + ". Skipping the record.");
				}
			}
		}

		/**
		 * Process a single record.
		 *
		 * @param record The record to be processed.
		 */
		private void processSingleRecord(Record record) {

			// Convert AWS Record in Spring Message.
			performSend(prepareMessageForRecord(record), record);
		}

		/**
		 * Checkpoint with retries.
		 *
		 * @param checkpointer checkpointer
		 */
		private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
			logger.info("Checkpointing shard " + kinesisShardId);
			for (int i = 0; i < NUM_RETRIES; i++) {
				try {
					checkpointer.checkpoint();
					break;
				}
				catch (ShutdownException se) {
					// Ignore checkpoint if the processor instance has been shutdown (fail over).
					logger.info("Caught shutdown exception, skipping checkpoint.", se);
					break;
				}
				catch (ThrottlingException e) {
					// Backoff and re-attempt checkpoint upon transient failures
					if (i >= (NUM_RETRIES - 1)) {
						logger.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
						break;
					}
					else {
						logger.info("Transient issue when checkpointing - attempt " + (i + 1) + " of " + NUM_RETRIES, e);
					}
				}
				catch (InvalidStateException e) {
					// This indicates an issue with the DynamoDB table (check for table, provisioned
					// IOPS).
					logger.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.",
							e);
					break;
				}
				try {
					Thread.sleep(KclMessageDrivenChannelAdapter.this.consumerBackoff);
				}
				catch (InterruptedException e) {
					logger.debug("Interrupted sleep", e);
				}
			}
		}

		@Override
		public void processRecords(ProcessRecordsInput processRecordsInput) {
			List<Record> records = processRecordsInput.getRecords();
			logger.debug("Processing " + records.size() + " records from " + kinesisShardId);

			// Process records and perform all exception handling.
			processRecordsWithRetries(records);

			// Checkpoint once every checkpoint interval.
			if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
				checkpoint(processRecordsInput.getCheckpointer());
				this.nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
			}

		}

		@Override
		public void shutdown(ShutdownInput shutdownInput) {
			logger.info("Shutting down record processor for shard: " + kinesisShardId);
			// Important to checkpoint after reaching end of shard, so we can start
			// processing data from child shards.
			if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
				checkpoint(shutdownInput.getCheckpointer());
			}
		}
	}
}
