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

package org.springframework.integration.aws;

import static cloud.localstack.TestUtils.DEFAULT_REGION;

import java.util.function.Supplier;

import cloud.localstack.TestUtils;
import cloud.localstack.docker.LocalstackDocker;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsAsyncClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;

/**
 * @author Artem Bilan
 *
 * @since 2.3
 */
public final class ExtendedDockerTestUtils {

	public static AmazonKinesisAsync getClientKinesisAsync() {
		AmazonKinesisAsyncClientBuilder amazonKinesisAsyncClientBuilder =
				AmazonKinesisAsyncClientBuilder.standard()
						.withEndpointConfiguration(
								createEndpointConfiguration(LocalstackDocker.INSTANCE::getEndpointKinesis));
		return applyConfigurationAndBuild(amazonKinesisAsyncClientBuilder);
	}

	public static AmazonDynamoDBAsync getClientDynamoDbAsync() {
		AmazonDynamoDBAsyncClientBuilder dynamoDBAsyncClientBuilder =
				AmazonDynamoDBAsyncClientBuilder.standard()
						.withEndpointConfiguration(
								createEndpointConfiguration(LocalstackDocker.INSTANCE::getEndpointDynamoDB));
		return applyConfigurationAndBuild(dynamoDBAsyncClientBuilder);
	}

	private static AwsClientBuilder.EndpointConfiguration createEndpointConfiguration(Supplier<String> supplier) {
		return new AwsClientBuilder.EndpointConfiguration(supplier.get(), DEFAULT_REGION);
	}

	private static <T, C extends AwsAsyncClientBuilder<C, T>> T applyConfigurationAndBuild(C builder) {
		return builder.withCredentials(TestUtils.getCredentialsProvider())
				.withClientConfiguration(new ClientConfiguration().withMaxErrorRetry(0).withConnectionTimeout(1000))
				.build();
	}

	private ExtendedDockerTestUtils() {
	}

}
