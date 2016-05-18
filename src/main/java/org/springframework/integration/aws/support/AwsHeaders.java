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

package org.springframework.integration.aws.support;

/**
 * The AWS specific message headers constants.
 *
 * @author Artem Bilan
 */
public abstract class AwsHeaders {

	private static final String PREFIX = "aws_";

	/**
	 * The {@value QUEUE} header for sending/receiving data over SQS.
	 */
	public static final String QUEUE = PREFIX + "queue";

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
	 * The {@value NOTIFICATION_STATUS} header for SNS notification status.
	 */
	public static final String NOTIFICATION_STATUS = PREFIX + "notificationStatus";

	/**
	 * The {@value SNS_MESSAGE_TYPE} header for SNS message type.
	 */
	public static final String SNS_MESSAGE_TYPE = PREFIX + "snsMessageType";

	/**
	 * The {@value SNS_PUBLISHED_MESSAGE_ID} header for message published over SNS.
	 */
	public static final String SNS_PUBLISHED_MESSAGE_ID = PREFIX + "snsPublishedMessageId";

}
