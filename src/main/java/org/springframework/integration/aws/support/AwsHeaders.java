/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.integration.aws.support;

/**
 * The AWS specific message headers constants.
 *
 * @author Artem Bilan
 */
public abstract class AwsHeaders {

	private static final String PREFIX = "aws_";

	/**
	 * The {@value QUEUE} header for sending data to SQS.
	 */
	public static final String QUEUE = PREFIX + "queue";

	/**
	 * The {@value RECEIVED_QUEUE} header for receiving data from SQS.
	 */
	public static final String RECEIVED_QUEUE = PREFIX + "receivedQueue";

	/**
	 * The {@value TOPIC} header for sending/receiving data over SNS.
	 */
	public static final String TOPIC = PREFIX + "topic";

	/**
	 * The {@value MESSAGE_ID} header for SQS/SNS message ids.
	 */
	public static final String MESSAGE_ID = PREFIX + "messageId";

	/**
	 * The {@value RECEIPT_HANDLE} header for received SQS message.
	 */
	public static final String RECEIPT_HANDLE = PREFIX + "receiptHandle";

	/**
	 * The {@value ACKNOWLEDGMENT} header for received SQS message.
	 */
	public static final String ACKNOWLEDGMENT = PREFIX + "acknowledgment";

	/**
	 * The {@value NOTIFICATION_STATUS} header for SNS notification status.
	 */
	public static final String NOTIFICATION_STATUS = PREFIX + "notificationStatus";

	/**
	 * The {@value SNS_MESSAGE_TYPE} header for SNS message type.
	 */
	public static final String SNS_MESSAGE_TYPE = PREFIX + "snsMessageType";

	/**
	 * The {@value SHARD} header to represent Kinesis shardId.
	 */
	public static final String SHARD = PREFIX + "shard";

	/**
	 * The {@value RECEIVED_STREAM} header for receiving data from Kinesis.
	 */
	public static final String RECEIVED_STREAM = PREFIX + "receivedStream";

	/**
	 * The {@value RECEIVED_PARTITION_KEY} header for receiving data from Kinesis.
	 */
	public static final String RECEIVED_PARTITION_KEY = PREFIX + "receivedPartitionKey";

	/**
	 * The {@value RECEIVED_SEQUENCE_NUMBER} header for receiving data from Kinesis.
	 */
	public static final String RECEIVED_SEQUENCE_NUMBER = PREFIX + "receivedSequenceNumber";

	/**
	 * The {@value STREAM} header for sending data to Kinesis.
	 */
	public static final String STREAM = PREFIX + "stream";

	/**
	 * The {@value PARTITION_KEY} header for sending data to Kinesis.
	 */
	public static final String PARTITION_KEY = PREFIX + "partitionKey";

	/**
	 * The {@value SEQUENCE_NUMBER} header for sending data to SQS/Kinesis.
	 */
	public static final String SEQUENCE_NUMBER = PREFIX + "sequenceNumber";

	/**
	 * The {@value CHECKPOINTER} header for checkpoint the shard sequenceNumber.
	 */
	public static final String CHECKPOINTER = PREFIX + "checkpointer";

	/**
	 * The {@value SERVICE_RESULT} header represents a {@link com.amazonaws.AmazonWebServiceResult}.
	 */
	public static final String SERVICE_RESULT = PREFIX + "serviceResult";

	/**
	 * The {@value RAW_RECORD} header represents received Kinesis record(s).
	 */
	public static final String RAW_RECORD = PREFIX + "rawRecord";

}
