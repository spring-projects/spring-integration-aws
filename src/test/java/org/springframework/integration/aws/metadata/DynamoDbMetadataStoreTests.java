/*
 * Copyright 2017-2020 the original author or authors.
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

package org.springframework.integration.aws.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.integration.aws.EnvironmentHostNameResolver;
import org.springframework.integration.aws.ExtendedDockerTestUtils;
import org.springframework.integration.test.util.TestUtils;

import cloud.localstack.docker.LocalstackDockerExtension;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.waiters.FixedDelayStrategy;
import com.amazonaws.waiters.MaxAttemptsRetryStrategy;
import com.amazonaws.waiters.PollingStrategy;
import com.amazonaws.waiters.Waiter;
import com.amazonaws.waiters.WaiterParameters;

/**
 * @author Artem Bilan
 *
 * @since 1.1
 */
@EnabledIfEnvironmentVariable(named = EnvironmentHostNameResolver.DOCKER_HOST_NAME, matches = ".+")
@ExtendWith(LocalstackDockerExtension.class)
@LocalstackDockerProperties(
		hostNameResolver = EnvironmentHostNameResolver.class,
		services = "dynamodb")
class DynamoDbMetadataStoreTests {

	private static final String TEST_TABLE = "testMetadataStore";

	private static AmazonDynamoDBAsync DYNAMO_DB;

	private static DynamoDbMetadataStore store;

	private final String file1 = "/remotepath/filesTodownload/file-1.txt";

	private final String file1Id = "12345";

	@BeforeAll
	static void setup() {
		DYNAMO_DB = ExtendedDockerTestUtils.getClientDynamoDbAsync();

		try {
			DYNAMO_DB.deleteTableAsync(TEST_TABLE);

			Waiter<DescribeTableRequest> waiter = DYNAMO_DB.waiters().tableNotExists();

			waiter.run(new WaiterParameters<>(new DescribeTableRequest(TEST_TABLE))
					.withPollingStrategy(
							new PollingStrategy(new MaxAttemptsRetryStrategy(25),
									new FixedDelayStrategy(1))));
		}
		catch (Exception e) {
			// Ignore
		}

		store = new DynamoDbMetadataStore(DYNAMO_DB, TEST_TABLE);
		store.setTimeToLive(10);
		store.afterPropertiesSet();
	}

	@BeforeEach
	void clear() throws InterruptedException {
		CountDownLatch createTableLatch = TestUtils.getPropertyValue(store, "createTableLatch", CountDownLatch.class);

		createTableLatch.await();

		DYNAMO_DB.deleteItem(TEST_TABLE, Collections.singletonMap("KEY", new AttributeValue().withS(this.file1)));
	}

	@Test
	void testGetFromStore() {
		String fileID = store.get(this.file1);
		assertThat(fileID).isNull();

		store.put(this.file1, this.file1Id);

		fileID = store.get(this.file1);
		assertThat(fileID).isNotNull();
		assertThat(fileID).isEqualTo(this.file1Id);
	}

	@Test
	void testPutIfAbsent() {
		String fileID = store.get(this.file1);
		assertThat(fileID).describedAs("Get First time, Value must not exist").isNull();

		fileID = store.putIfAbsent(this.file1, this.file1Id);
		assertThat(fileID).describedAs("Insert First time, Value must return null").isNull();

		fileID = store.putIfAbsent(this.file1, "56789");
		assertThat(fileID).describedAs("Key Already Exists - Insertion Failed, ol value must be returned").isNotNull();
		assertThat(fileID).describedAs("The Old Value must be equal to returned").isEqualTo(this.file1Id);

		assertThat(store.get(this.file1)).describedAs("The Old Value must return").isEqualTo(this.file1Id);
	}

	@Test
	void testRemove() {
		String fileID = store.remove(this.file1);
		assertThat(fileID).isNull();

		fileID = store.putIfAbsent(this.file1, this.file1Id);
		assertThat(fileID).isNull();

		fileID = store.remove(this.file1);
		assertThat(fileID).isNotNull();
		assertThat(fileID).isEqualTo(this.file1Id);

		fileID = store.get(this.file1);
		assertThat(fileID).isNull();
	}

	@Test
	void testReplace() {
		boolean removedValue = store.replace(this.file1, this.file1Id, "4567");
		assertThat(removedValue).isFalse();

		String fileID = store.get(this.file1);
		assertThat(fileID).isNull();

		fileID = store.putIfAbsent(this.file1, this.file1Id);
		assertThat(fileID).isNull();

		removedValue = store.replace(this.file1, this.file1Id, "4567");
		assertThat(removedValue).isTrue();

		fileID = store.get(this.file1);
		assertThat(fileID).isNotNull();
		assertThat(fileID).isEqualTo("4567");
	}

}
