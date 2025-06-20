/*
 * Copyright 2025-present the original author or authors.
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

import java.io.Serial;

import com.amazonaws.services.kinesis.producer.UserRecord;

/**
 * An exception triggered from the {@link org.springframework.integration.aws.outbound.KplMessageHandler}
 * while sending records to Kinesis when maximum number of records in flight exceeds the backpressure threshold.
 *
 * @author Siddharth Jain
 * @author Artem Bilan
 *
 * @since 3.0.9
 */
public class KplBackpressureException extends RuntimeException {

	@Serial
	private static final long serialVersionUID = 1L;

	private final transient UserRecord userRecord;

	public KplBackpressureException(String message, UserRecord userRecord) {
		super(message);
		this.userRecord = userRecord;
	}

	/**
	 * Get the {@link UserRecord} when this exception has been thrown.
	 * @return the {@link UserRecord} when this exception has been thrown.
	 */
	public UserRecord getUserRecord() {
		return this.userRecord;
	}

}
