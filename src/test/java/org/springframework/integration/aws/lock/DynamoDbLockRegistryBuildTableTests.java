/*
 * Copyright 2020-2020 the original author or authors.
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

package org.springframework.integration.aws.lock;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.AbstractAmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;

/**
 * @author Asiel Caballero
 *
 * @since 2.3.5
 */
class DynamoDbLockRegistryBuildTableTests {
	private static final String TEST_TABLE
			= "testLockRegistry" + DynamoDbLockRegistryBuildTableTests.class.getSimpleName();

	private InMemoryAmazonDynamoDB client;

	@BeforeEach
	void setup() {
		this.client = new InMemoryAmazonDynamoDB();
	}

	@Test
	void onDemandIsSetup() throws InterruptedException {
		assertsBillingMode(BillingMode.PAY_PER_REQUEST,
				lockRegistry -> lockRegistry.setBillingMode(BillingMode.PAY_PER_REQUEST));
	}

	@Test
	void provisionedIsSetup() throws InterruptedException {
		assertsBillingMode(BillingMode.PROVISIONED,
				lockRegistry -> lockRegistry.setBillingMode(BillingMode.PROVISIONED));
	}

	@Test
	void defaultsToProvisioned() throws InterruptedException {
		assertsBillingMode(BillingMode.PAY_PER_REQUEST, store -> { });
	}

	private void assertsBillingMode(com.amazonaws.services.dynamodbv2.model.BillingMode billingMode,
			Consumer<DynamoDbLockRegistry> propertySetter) throws InterruptedException {
		DynamoDbLockRegistry lockRegistry = new DynamoDbLockRegistry(this.client, TEST_TABLE);
		propertySetter.accept(lockRegistry);
		lockRegistry.afterPropertiesSet();

		lockRegistry.obtain("test").tryLock(1, TimeUnit.SECONDS);
		assertThat(billingMode.toString())
				.isEqualTo(this.client.getCreateTableRequest().getBillingMode());
	}

	private static class InMemoryAmazonDynamoDB extends AbstractAmazonDynamoDB {
		private CreateTableRequest createTableRequest;
		private boolean wasCalled = false;

		@Override
		public synchronized CreateTableResult createTable(CreateTableRequest request) {
			this.createTableRequest = request;

			return null;
		}

		@Override
		public GetItemResult getItem(GetItemRequest request) {
			return new GetItemResult();
		}

		@Override
		public PutItemResult putItem(PutItemRequest request) {
			return new PutItemResult();
		}

		@Override
		public synchronized DescribeTableResult describeTable(DescribeTableRequest request) {
			if (this.wasCalled) {
				return new DescribeTableResult()
						.withTable(new TableDescription()
								.withTableStatus(TableStatus.ACTIVE));
			}
			else {
				this.wasCalled = true;
				throw new ResourceNotFoundException(TEST_TABLE);
			}
		}

		public synchronized CreateTableRequest getCreateTableRequest() {
			return this.createTableRequest;
		}
	}
}
