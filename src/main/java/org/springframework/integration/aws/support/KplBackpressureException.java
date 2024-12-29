/*
 * Copyright 2017-2024 the original author or authors.
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

package org.springframework.integration.aws.support;

import com.amazonaws.services.kinesis.producer.UserRecord;

/**
 * An exception triggered from {@link org.springframework.integration.aws.outbound.KplMessageHandler} while sending
 * records to kinesis when maximum number of records in flight exceeds the backpressure threshold.
 *
 * @author Siddharth Jain
 *
 * @since 3.0.9
 */
public class KplBackpressureException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	private final UserRecord userRecord;

	public KplBackpressureException(String message, UserRecord userRecord) {
		super(message);
		this.userRecord = userRecord;
	}

	/**
	 * Get the {@link UserRecord} related.
	 * @return {@link UserRecord} linked while sending the record to kinesis.
	 */
	public UserRecord getUserRecord() {
		return this.userRecord;
	}
}
