/*
 * Copyright 2024-present the original author or authors.
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

import java.util.List;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.services.kinesis.model.KinesisResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

/**
 * The {@link KinesisResponse} adapter for the KPL {@link UserRecordResult} response.
 *
 * @author Artem Bilan
 *
 * @since 3.0.8
 */
public class UserRecordResponse extends KinesisResponse {

	private final String shardId;

	private final String sequenceNumber;

	private final List<Attempt> attempts;

	public UserRecordResponse(UserRecordResult userRecordResult) {
		super(PutRecordResponse.builder());
		this.shardId = userRecordResult.getShardId();
		this.sequenceNumber = userRecordResult.getSequenceNumber();
		this.attempts = userRecordResult.getAttempts();
	}

	public String shardId() {
		return this.shardId;
	}

	public String sequenceNumber() {
		return this.sequenceNumber;
	}

	public List<Attempt> attempts() {
		return this.attempts;
	}

	@Override
	public AwsResponse.Builder toBuilder() {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<SdkField<?>> sdkFields() {
		throw new UnsupportedOperationException();
	}

}
