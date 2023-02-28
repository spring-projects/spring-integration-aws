/*
 * Copyright 2018-2023 the original author or authors.
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

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.model.TimeToLiveSpecification;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

/**
 * Encapsulation of the DynamoDB shunting that is needed for locks.
 * <p>
 * The DynamoDb table must have these attributes:
 * <ul>
 * <li> {@link DynamoDbLockRepository#KEY_ATTR} {@link ScalarAttributeType#S} - partition key {@link KeyType#HASH}
 * <li> {@link DynamoDbLockRepository#OWNER_ATTR} {@link ScalarAttributeType#S}
 * <li> {@link DynamoDbLockRepository#CREATED_ATTR} {@link ScalarAttributeType#N}
 * <li> {@link DynamoDbLockRepository#TTL_ATTR} {@link ScalarAttributeType#N}
 * </ul>
 *
 * @author Artem Bilan
 *
 * @since 3.0
 */
public class DynamoDbLockRepository implements InitializingBean, DisposableBean, Closeable {

	/**
	 * The {@value DEFAULT_TABLE_NAME} default name for the locks table in the DynamoDB.
	 */
	public static final String DEFAULT_TABLE_NAME = "SpringIntegrationLockRegistry";

	/**
	 * The {@value KEY_ATTR} name for the partition key in the table.
	 */
	public static final String KEY_ATTR = "lockKey";

	/**
	 * The {@value OWNER_ATTR} name for the owner of lock in the table.
	 */
	public static final String OWNER_ATTR = "lockOwner";

	/**
	 * The {@value CREATED_ATTR} date for lock item.
	 */
	public static final String CREATED_ATTR = "createdAt";

	/**
	 * The {@value TTL_ATTR} for how long the lock is valid.
	 */
	public static final String TTL_ATTR = "expireAt";

	private static final String LOCK_EXISTS_EXPRESSION =
			String.format("attribute_exists(%s) AND %s = :owner", KEY_ATTR, OWNER_ATTR);

	private static final String LOCK_NOT_EXISTS_EXPRESSION =
			String.format("attribute_not_exists(%s) OR %s < :ttl OR (%s)", KEY_ATTR, TTL_ATTR, LOCK_EXISTS_EXPRESSION);

	/**
	 * Default value for the {@link #leaseDuration} property.
	 */
	public static final Duration DEFAULT_LEASE_DURATION = Duration.ofSeconds(60);

	private static final Log LOGGER = LogFactory.getLog(DynamoDbLockRegistry.class);

	private final ThreadFactory customizableThreadFactory = new CustomizableThreadFactory("dynamodb-lock-registry-");

	private final CountDownLatch createTableLatch = new CountDownLatch(1);

	private final Set<String> heldLocks = Collections.synchronizedSet(new HashSet<>());

	private final AmazonDynamoDB dynamoDB;

	private final Table lockTable;

	private BillingMode billingMode = BillingMode.PAY_PER_REQUEST;

	private long readCapacity = 1L;

	private long writeCapacity = 1L;

	private String owner = UUID.randomUUID().toString();

	private Duration leaseDuration = DEFAULT_LEASE_DURATION;

	private Map<String, Object> ownerAttribute;

	private volatile boolean initialized;

	public DynamoDbLockRepository(AmazonDynamoDB dynamoDB) {
		this(dynamoDB, DEFAULT_TABLE_NAME);
	}

	public DynamoDbLockRepository(AmazonDynamoDB dynamoDB, String tableName) {
		this.dynamoDB = dynamoDB;
		this.lockTable = new Table(this.dynamoDB, tableName);
	}

	public void setBillingMode(BillingMode billingMode) {
		Assert.notNull(billingMode, "'billingMode' must not be null");
		this.billingMode = billingMode;
	}

	public void setReadCapacity(long readCapacity) {
		this.readCapacity = readCapacity;
	}

	public void setWriteCapacity(long writeCapacity) {
		this.writeCapacity = writeCapacity;
	}

	/**
	 * Specify a custom client id (owner) for locks in DB.
	 * Must be unique per cluster to avoid interlocking between different instances.
	 * @param owner the client id to be associated with locks handled by the repository.
	 */
	public void setOwner(String owner) {
		this.owner = owner;
	}

	/**
	 * How long to hold the item after last update.
	 * @param leaseDuration the duration for how long to keep the lock after last update.
	 */
	public void setLeaseDuration(Duration leaseDuration) {
		this.leaseDuration = leaseDuration;
	}

	public String getTableName() {
		return this.lockTable.getTableName();
	}

	public String getOwner() {
		return this.owner;
	}

	@Override
	public void afterPropertiesSet() {
		this.customizableThreadFactory
				.newThread(() -> {
					try {
						if (!lockTableExists()) {
							if (LOGGER.isInfoEnabled()) {
								LOGGER.info("No table '" + getTableName() + "'. Creating one...");
							}
							createLockTableInDynamoDB();
							int i = 0;
							// We need up to one minute to wait until table is created on AWS.
							while (i++ < 60) {
								if (lockTableExists()) {
									this.dynamoDB.updateTimeToLive(
											new UpdateTimeToLiveRequest()
													.withTableName(getTableName())
													.withTimeToLiveSpecification(
															new TimeToLiveSpecification()
																	.withEnabled(true)
																	.withAttributeName(TTL_ATTR)));
									return;
								}
								else {
									try {
										// This is allowed minimum for constant AWS requests.
										Thread.sleep(1000);
									}
									catch (InterruptedException e) {
										ReflectionUtils.rethrowRuntimeException(e);
									}
								}
							}

							LOGGER.error("Cannot describe DynamoDb table: " + getTableName());
						}
					}
					finally {
						// Release create table barrier either way.
						// If there is an error during creation/description,
						// we defer the actual ResourceNotFoundException to the end-user active
						// calls.
						this.createTableLatch.countDown();
					}
				})
				.start();


		this.ownerAttribute = Map.of(":owner", this.owner);
		this.initialized = true;
	}

	private boolean lockTableExists() {
		try {
			DescribeTableResult result = this.dynamoDB.describeTable(new DescribeTableRequest(getTableName()));
			return Set.of(TableStatus.ACTIVE, TableStatus.UPDATING)
					.contains(TableStatus.fromValue(result.getTable().getTableStatus()));
		}
		catch (ResourceNotFoundException e) {
			// This exception indicates the table doesn't exist.
			return false;
		}
	}

	/**
	 * Creates a DynamoDB table with the right schema for it to be used by this locking library.
	 * The table should be set up in advance,
	 * because it takes a few minutes for DynamoDB to provision a new instance.
	 * If table already exists no exception.
	 */
	private void createLockTableInDynamoDB() {
		try {
			CreateTableRequest createTableRequest =
					new CreateTableRequest()
							.withTableName(getTableName())
							.withKeySchema(new KeySchemaElement(KEY_ATTR, KeyType.HASH))
							.withAttributeDefinitions(new AttributeDefinition(KEY_ATTR, ScalarAttributeType.S))
							.withBillingMode(this.billingMode);

			if (BillingMode.PROVISIONED.equals(this.billingMode)) {
				createTableRequest.setProvisionedThroughput(
						new ProvisionedThroughput(this.readCapacity, this.writeCapacity));
			}

			this.dynamoDB.createTable(createTableRequest);
		}
		catch (ResourceInUseException ex) {
			// Swallow an exception and you should check for table existence
		}
	}

	private void awaitForActive() {
		Assert.state(this.initialized,
				() -> "The component has not been initialized: " + this + ".\n Is it declared as a bean?");

		IllegalStateException illegalStateException = new IllegalStateException(
				"The DynamoDb table " + getTableName() + " has not been created during " + 60 + " seconds");
		try {
			if (!this.createTableLatch.await(60, TimeUnit.SECONDS)) {
				throw illegalStateException;
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw illegalStateException;
		}
	}

	/**
	 * Check if a lock is held by this repository.
	 * @param lock the lock to check.
	 * @return acquired or not.
	 */
	public boolean isAcquired(String lock) {
		awaitForActive();
		if (this.heldLocks.contains(lock)) {
			QuerySpec querySpec =
					new QuerySpec()
							.withHashKey(KEY_ATTR, lock)
							.withProjectionExpression(KEY_ATTR)
							.withMaxResultSize(1)
							.withFilterExpression(OWNER_ATTR + " = :owner AND " + TTL_ATTR + " >= :ttl")
							.withValueMap(ownerWithTtlValues(currentEpochSeconds()));

			return this.lockTable.query(querySpec).iterator().hasNext();
		}
		return false;
	}

	/**
	 * Remove a lock from this repository.
	 * @param lock the lock to remove.
	 */
	public void delete(String lock) {
		awaitForActive();
		if (this.heldLocks.remove(lock)) {
			deleteFromDb(lock);
		}
	}

	private void deleteFromDb(String lock) {
		doDelete(
				new DeleteItemSpec()
						.withPrimaryKey(KEY_ATTR, lock)
						.withConditionExpression(OWNER_ATTR + " = :owner")
						.withValueMap(this.ownerAttribute));
	}

	private void doDelete(DeleteItemSpec deleteItemSpec) {
		try {
			this.lockTable.deleteItem(deleteItemSpec);
		}
		catch (ConditionalCheckFailedException ex) {
			// Ignore - assuming no record in DB anymore.
		}
	}

	/**
	 * Remove all the expired locks.
	 */
	public void deleteExpired() {
		awaitForActive();
		synchronized (this.heldLocks) {
			this.heldLocks.forEach((lock) ->
					doDelete(
							new DeleteItemSpec()
									.withPrimaryKey(KEY_ATTR, lock)
									.withConditionExpression(OWNER_ATTR + " = :owner AND " + TTL_ATTR + " < :ttl")
									.withValueMap(ownerWithTtlValues(currentEpochSeconds()))));
			this.heldLocks.clear();
		}
	}

	private ValueMap ownerWithTtlValues(long epochSeconds) {
		ValueMap valueMap =
				new ValueMap()
						.withNumber(":ttl", epochSeconds);
		valueMap.putAll(this.ownerAttribute);
		return valueMap;
	}

	/**
	 * Acquire a lock for a key.
	 * @param lock the key for lock to acquire.
	 * @return acquired or not.
	 */
	public boolean acquire(String lock) {
		awaitForActive();
		long currentTime = currentEpochSeconds();
		PutItemSpec putItemSpec =
				new PutItemSpec()
						.withItem(
								new Item()
										.withPrimaryKey(KEY_ATTR, lock)
										.withString(OWNER_ATTR, this.owner)
										.withLong(CREATED_ATTR, currentTime)
										.withLong(TTL_ATTR, ttlEpochSeconds()))
						.withConditionExpression(LOCK_NOT_EXISTS_EXPRESSION)
						.withValueMap(ownerWithTtlValues(currentTime));
		try {
			this.lockTable.putItem(putItemSpec);
			this.heldLocks.add(lock);
			return true;
		}
		catch (ConditionalCheckFailedException ex) {
			return false;
		}
	}

	/**
	 * Renew the lease for a lock.
	 * @param lock the lock to renew.
	 * @return renewed or not.
	 */
	public boolean renew(String lock) {
		awaitForActive();
		if (this.heldLocks.contains(lock)) {
			UpdateItemSpec updateItemSpec =
					new UpdateItemSpec()
							.withPrimaryKey(KEY_ATTR, lock)
							.withUpdateExpression("SET " + TTL_ATTR + " = :ttl")
							.withConditionExpression(LOCK_EXISTS_EXPRESSION)
							.withValueMap(ownerWithTtlValues(ttlEpochSeconds()));
			try {
				this.lockTable.updateItem(updateItemSpec);
				return true;
			}
			catch (ConditionalCheckFailedException ex) {
				return false;
			}
		}
		return false;
	}

	@Override
	public void destroy() {
		close();
	}

	@Override
	public void close() {
		synchronized (this.heldLocks) {
			this.heldLocks.forEach(this::deleteFromDb);
			this.heldLocks.clear();
		}
	}

	private long ttlEpochSeconds() {
		return Instant.now().plus(this.leaseDuration).getEpochSecond();
	}

	private static long currentEpochSeconds() {
		return Instant.now().getEpochSecond();
	}

}
