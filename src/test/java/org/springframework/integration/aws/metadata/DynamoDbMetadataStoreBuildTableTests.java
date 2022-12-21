/*
 * Copyright 2020-2022 the original author or authors.
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

import java.util.concurrent.Future;
import java.util.function.Consumer;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AbstractAmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Asiel Caballero
 *
 * @since 2.3.5
 */
class DynamoDbMetadataStoreBuildTableTests {

	private static final String TEST_TABLE
			= "testMetadataStore" + DynamoDbMetadataStoreBuildTableTests.class.getSimpleName();

	private final InMemoryAmazonDynamoDB client = new InMemoryAmazonDynamoDB();

	@Test
	void onDemandIsSetup() {
		assertsBillingMode(BillingMode.PAY_PER_REQUEST,
				store -> store.setBillingMode(BillingMode.PAY_PER_REQUEST));
	}

	@Test
	void provisionedIsSetup() {
		assertsBillingMode(BillingMode.PROVISIONED,
				store -> store.setBillingMode(BillingMode.PROVISIONED));
	}

	@Test
	void defaultsToProvisioned() {
		assertsBillingMode(BillingMode.PAY_PER_REQUEST, store -> { });
	}

	private void assertsBillingMode(com.amazonaws.services.dynamodbv2.model.BillingMode billingMode,
			Consumer<DynamoDbMetadataStore> propertySetter) {
		DynamoDbMetadataStore store = new DynamoDbMetadataStore(this.client, TEST_TABLE);
		propertySetter.accept(store);
		store.afterPropertiesSet();

		assertThat(billingMode.toString())
				.isEqualTo(this.client.createTableRequest.getBillingMode());
	}

	private static class InMemoryAmazonDynamoDB extends AbstractAmazonDynamoDBAsync {
		private CreateTableRequest createTableRequest;

		@Override
		public Future<CreateTableResult> createTableAsync(CreateTableRequest request,
				AsyncHandler<CreateTableRequest, CreateTableResult> asyncHandler) {
			this.createTableRequest = request;

			return null;
		}

		@Override
		public DescribeTableResult describeTable(DescribeTableRequest request) {
			throw new ResourceNotFoundException(TEST_TABLE);
		}
	}
}
