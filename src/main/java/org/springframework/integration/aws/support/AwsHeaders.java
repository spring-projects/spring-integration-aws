/*
 * Copyright 2015 the original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.springframework.integration.aws.support;

/**
 * @author Artem Bilan
 */
public abstract class AwsHeaders {

	private static final String PREFIX = "aws_";

	public static final String QUEUE = PREFIX + "queue";

	public static final String MESSAGE_ID = PREFIX + "messageId";

	public static final String RECEIPT_HANDLE = PREFIX + "receiptHandle";

}
