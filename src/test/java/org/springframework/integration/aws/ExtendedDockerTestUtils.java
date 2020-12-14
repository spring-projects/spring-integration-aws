/*
 * Copyright 2019-2020 the original author or authors.
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

import static cloud.localstack.Constants.DEFAULT_REGION;

import java.util.function.Supplier;

import cloud.localstack.Localstack;
import cloud.localstack.awssdkv1.TestUtils;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsAsyncClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsync;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;

/**
 * An utility class for providing AWS {@code async} clients based on the Local Stack Docker instance.
 * In addition it provides SSL-based async clients.
 *
 *
 * @author Artem Bilan
 *
 * @since 2.3
 */
public final class ExtendedDockerTestUtils {

	public static AmazonKinesisAsync getClientKinesisAsync() {
		return doGetClientKinesisAsync(false);
	}

	public static AmazonKinesisAsync getClientKinesisAsyncSsl() {
		return doGetClientKinesisAsync(true);
	}

	private static AmazonKinesisAsync doGetClientKinesisAsync(boolean ssl) {
		AmazonKinesisAsyncClientBuilder amazonKinesisAsyncClientBuilder =
				AmazonKinesisAsyncClientBuilder.standard()
						.withEndpointConfiguration(
								createEndpointConfiguration(Localstack.INSTANCE::getEndpointKinesis, ssl));
		return applyConfigurationAndBuild(amazonKinesisAsyncClientBuilder);
	}

	public static AmazonDynamoDBAsync getClientDynamoDbAsync() {
		return doClientDynamoDbAsync(false);
	}

	public static AmazonDynamoDBAsync getClientDynamoDbAsyncSsl() {
		return doClientDynamoDbAsync(true);
	}

	private static AmazonDynamoDBAsync doClientDynamoDbAsync(boolean ssl) {
		AmazonDynamoDBAsyncClientBuilder dynamoDBAsyncClientBuilder =
				AmazonDynamoDBAsyncClientBuilder.standard()
						.withEndpointConfiguration(
								createEndpointConfiguration(Localstack.INSTANCE::getEndpointDynamoDB, ssl));
		return applyConfigurationAndBuild(dynamoDBAsyncClientBuilder);
	}

	public static AmazonCloudWatchAsync getClientCloudWatchAsync() {
		return doClientCloudWatchAsync(false);
	}

	public static AmazonCloudWatchAsync getClientCloudWatchAsyncSsl() {
		return doClientCloudWatchAsync(true);
	}

	private static AmazonCloudWatchAsync doClientCloudWatchAsync(boolean ssl) {
		AmazonCloudWatchAsyncClientBuilder cloudWatchAsyncClientBuilder =
				AmazonCloudWatchAsyncClientBuilder.standard()
						.withEndpointConfiguration(
								createEndpointConfiguration(Localstack.INSTANCE::getEndpointCloudWatch, ssl));
		return applyConfigurationAndBuild(cloudWatchAsyncClientBuilder);
	}

	private static AwsClientBuilder.EndpointConfiguration createEndpointConfiguration(Supplier<String> supplier,
			boolean ssl) {

		String serviceEndpoint = supplier.get();
		if (ssl) {
			serviceEndpoint = serviceEndpoint.replaceFirst("http", "https");
		}
		return new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, DEFAULT_REGION);
	}

	private static <T, C extends AwsAsyncClientBuilder<C, T>> T applyConfigurationAndBuild(C builder) {
		return builder.withCredentials(TestUtils.getCredentialsProvider())
				.withClientConfiguration(new ClientConfiguration().withMaxErrorRetry(0).withConnectionTimeout(1000))
				.build();
	}

	private ExtendedDockerTestUtils() {
	}

}
