/*
 * Copyright 2022 the original author or authors.
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

import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;

/**
 * The base contract for JUnit tests based on the container for Localstack.
 * The Testcontainers 'reuse' option must be disabled,so, Ryuk container is started
 * and will clean all the containers up from this test suite after JVM exit.
 * Since the Localstack container instance is shared via static property, it is going to be
 * started only once per JVM, therefore the target Docker container is reused automatically.
 *
 * @author Artem Bilan
 * @since 3.0
 */
@Testcontainers(disabledWithoutDocker = true)
public interface LocalstackContainerTest {

	LocalStackContainer LOCAL_STACK_CONTAINER =
			new LocalStackContainer(
					DockerImageName.parse("localstack/localstack:1.2.0"))
					.withServices(
							LocalStackContainer.Service.DYNAMODB,
							LocalStackContainer.Service.KINESIS,
							LocalStackContainer.Service.CLOUDWATCH);

	@BeforeAll
	static void startContainer() {
		LOCAL_STACK_CONTAINER.start();
	}

	static AmazonDynamoDBAsync dynamoDbClient() {
		return applyAwsClientOptions(AmazonDynamoDBAsyncClientBuilder.standard(), LocalStackContainer.Service.DYNAMODB);
	}

	static AmazonKinesisAsync kinesisClient() {
		return applyAwsClientOptions(AmazonKinesisAsyncClientBuilder.standard(), LocalStackContainer.Service.KINESIS);
	}

	static AmazonCloudWatch cloudWatchClient() {
		return applyAwsClientOptions(AmazonCloudWatchClientBuilder.standard(), LocalStackContainer.Service.CLOUDWATCH);
	}

	static AWSCredentialsProvider credentialsProvider() {
		return new AWSStaticCredentialsProvider(
				new BasicAWSCredentials(
						LOCAL_STACK_CONTAINER.getAccessKey(),
						LOCAL_STACK_CONTAINER.getSecretKey()));
	}

	private static <B extends AwsClientBuilder<B, T>, T> T applyAwsClientOptions(B clientBuilder,
			LocalStackContainer.Service serviceToBuild) {

		return clientBuilder.withEndpointConfiguration(
						new AwsClientBuilder.EndpointConfiguration(
								LOCAL_STACK_CONTAINER.getEndpointOverride(serviceToBuild).toString(),
								LOCAL_STACK_CONTAINER.getRegion()))
				.withCredentials(credentialsProvider())
				.build();
	}

}
