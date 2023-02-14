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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LockCurrentlyUnavailableException;
import com.amazonaws.services.dynamodbv2.model.LockNotGrantedException;
import com.amazonaws.services.dynamodbv2.model.LockTableDoesNotExistException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.integration.support.locks.ExpirableLockRegistry;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

/**
 * An {@link ExpirableLockRegistry} implementation for the AWS DynamoDB. The algorithm is
 * based on the {@link AmazonDynamoDBLockClient}.
 * <p>
 * Can create table in DynamoDB if an external {@link AmazonDynamoDBLockClient} is not
 * provided.
 *
 * @author Artem Bilan
 * @author Karl Lessard
 * @author Asiel Caballero
 * @since 2.0
 */
public class DynamoDbLockRegistry implements ExpirableLockRegistry, InitializingBean, DisposableBean {

	/**
	 * The {@value DEFAULT_TABLE_NAME} default name for the locks table in the DynamoDB.
	 */
	public static final String DEFAULT_TABLE_NAME = "SpringIntegrationLockRegistry";

	/**
	 * The {@value DEFAULT_PARTITION_KEY_NAME} default name for the partition key in the
	 * table.
	 */
	public static final String DEFAULT_PARTITION_KEY_NAME = "lockKey";

	/**
	 * The {@value DEFAULT_SORT_KEY_NAME} default name for the sort key in the table.
	 */
	public static final String DEFAULT_SORT_KEY_NAME = "sortKey";

	/**
	 * The {@value DEFAULT_SORT_KEY} default value for the sort key in the table.
	 */
	public static final String DEFAULT_SORT_KEY = "SpringIntegrationLocks";

	/**
	 * The {@value DEFAULT_REFRESH_PERIOD_MS} default period in milliseconds between DB
	 * polling requests.
	 */
	public static final long DEFAULT_REFRESH_PERIOD_MS = 1000L;

	private static final Log logger = LogFactory.getLog(DynamoDbLockRegistry.class);

	private final ThreadFactory customizableThreadFactory = new CustomizableThreadFactory("dynamodb-lock-registry-");

	private final Map<String, DynamoDbLock> locks = new ConcurrentHashMap<>();

	private final CountDownLatch createTableLatch = new CountDownLatch(1);

	private final AmazonDynamoDB dynamoDB;

	private final String tableName;

	private AmazonDynamoDBLockClient dynamoDBLockClient;

	private boolean dynamoDBLockClientExplicitlySet;

	private BillingMode billingMode = BillingMode.PAY_PER_REQUEST;

	private long readCapacity = 1L;

	private long writeCapacity = 1L;

	private String partitionKey = DEFAULT_PARTITION_KEY_NAME;

	private String sortKeyName = DEFAULT_SORT_KEY_NAME;

	private String sortKey = DEFAULT_SORT_KEY;

	private long refreshPeriod = DEFAULT_REFRESH_PERIOD_MS;

	private long leaseDuration = 20L;

	private long heartbeatPeriod = 5L;

	private volatile boolean initialized;

	public DynamoDbLockRegistry(AmazonDynamoDB dynamoDB) {
		this(dynamoDB, DEFAULT_TABLE_NAME);
	}

	public DynamoDbLockRegistry(AmazonDynamoDB dynamoDB, String tableName) {
		Assert.notNull(dynamoDB, "'dynamoDB' must not be null");
		Assert.hasText(tableName, "'tableName' must not be empty");

		this.dynamoDB = dynamoDB;
		this.tableName = tableName;
	}

	public DynamoDbLockRegistry(AmazonDynamoDBLockClient dynamoDBLockClient) {
		Assert.notNull(dynamoDBLockClient, "'dynamoDBLockClient' must not be null");

		this.dynamoDBLockClient = dynamoDBLockClient;
		this.dynamoDBLockClientExplicitlySet = true;
		this.dynamoDB = null;
		this.tableName = null;
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

	public void setPartitionKey(String partitionKey) {
		Assert.hasText(partitionKey, "'partitionKey' must not be empty");
		this.partitionKey = partitionKey;
	}

	/**
	 * Specify a name of the table attribute which is used as a sort key.
	 * @param sortKeyName the sort key attribute name to use.
	 */
	public void setSortKeyName(String sortKeyName) {
		this.sortKeyName = sortKeyName;
	}

	/**
	 * Specify a value for the sort key attribute of the lock item.
	 * @param sortKey the sort key value to use.
	 */
	public void setSortKey(String sortKey) {
		this.sortKey = sortKey;
	}

	public void setLeaseDuration(long leaseDuration) {
		this.leaseDuration = leaseDuration;
	}

	/**
	 * Specify a period in seconds how often send locks renewal requests called heartbeat.
	 * When the value is less than or equal to {@code 0}, the heartbeat is disabled.
	 * @param heartbeatPeriod the heartbeat period for background thread to renew locks in DB
	 */
	public void setHeartbeatPeriod(long heartbeatPeriod) {
		this.heartbeatPeriod = heartbeatPeriod;
	}

	public void setRefreshPeriod(long refreshPeriod) {
		this.refreshPeriod = refreshPeriod;
	}

	/**
	 * Set the {@link Executor}, where is not provided then a default of cached thread
	 * pool Executor will be used.
	 * @param executor the executor service
	 * @deprecated with no-op in favor of internally created unmanaged threads.
	 */
	@Deprecated
	public void setExecutor(Executor executor) {
	}

	@Override
	public void afterPropertiesSet() {
		if (!this.dynamoDBLockClientExplicitlySet) {
			AmazonDynamoDBLockClientOptions dynamoDBLockClientOptions = AmazonDynamoDBLockClientOptions
					.builder(this.dynamoDB, this.tableName).withPartitionKeyName(this.partitionKey)
					.withSortKeyName(this.sortKeyName)
					.withCreateHeartbeatBackgroundThread(this.heartbeatPeriod > 0)
					.withHeartbeatPeriod(this.heartbeatPeriod)
					.withLeaseDuration(this.leaseDuration).build();

			this.dynamoDBLockClient = new AmazonDynamoDBLockClient(dynamoDBLockClientOptions);
		}

		this.leaseDuration = (long) new DirectFieldAccessor(this.dynamoDBLockClient)
				.getPropertyValue("leaseDurationInMilliseconds");

		this.customizableThreadFactory
				.newThread(() -> {
					try {
						if (!this.dynamoDBLockClientExplicitlySet) {
							try {
								this.dynamoDBLockClient.assertLockTableExists();
								return;
							}
							catch (LockTableDoesNotExistException e) {
								if (logger.isInfoEnabled()) {
									logger.info("No table '" + this.tableName + "'. Creating one...");
								}
							}

							createLockTableInDynamoDB();
						}

						int i = 0;
						// We need up to one minute to wait until table is created on AWS.
						while (i++ < 60) {
							if (this.dynamoDBLockClient.lockTableExists()) {
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

						logger.error("Cannot describe DynamoDb table: " + this.tableName);
					}
					finally {
						// Release create table barrier either way.
						// If there is an error during creation/description,
						// we deffer the actual ResourceNotFoundException to the end-user active
						// calls.
						this.createTableLatch.countDown();
					}
				})
				.start();

		this.initialized = true;
	}

	/**
	 * Creates a DynamoDB table with the right schema for it to be used by this locking library.
	 * The table should be set
	 * up in advance, because it takes a few minutes for DynamoDB to provision a new instance.
	 * <p>
	 * This method is a variation of {@link AmazonDynamoDBLockClient#createLockTableInDynamoDB} to support custom
	 * {@link BillingMode} for the lock table.
	 * <p>
	 * If table already exists no exception.
	 */
	private void createLockTableInDynamoDB() {
		try {
			KeySchemaElement partitionKeyElement = new KeySchemaElement();
			partitionKeyElement.setAttributeName(this.partitionKey);
			partitionKeyElement.setKeyType(KeyType.HASH);

			List<KeySchemaElement> keySchema = new ArrayList<>();
			keySchema.add(partitionKeyElement);

			Collection<AttributeDefinition> attributeDefinitions = new ArrayList<>();
			attributeDefinitions.add(new AttributeDefinition().withAttributeName(this.partitionKey)
					.withAttributeType(ScalarAttributeType.S));

			KeySchemaElement sortKeyElement = new KeySchemaElement();
			sortKeyElement.setAttributeName(this.sortKeyName);
			sortKeyElement.setKeyType(KeyType.RANGE);
			keySchema.add(sortKeyElement);
			attributeDefinitions.add(new AttributeDefinition().withAttributeName(this.sortKeyName)
					.withAttributeType(ScalarAttributeType.S));

			CreateTableRequest createTableRequest = new CreateTableRequest(this.tableName, keySchema)
					.withAttributeDefinitions(attributeDefinitions)
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
				"The DynamoDb table " + this.tableName + " has not been created during " + 60 + " seconds");
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

	@Override
	public void destroy() throws Exception {
		if (!this.dynamoDBLockClientExplicitlySet) {
			this.dynamoDBLockClient.close();
		}
	}

	@Override
	public Lock obtain(Object lockKey) {
		Assert.isInstanceOf(String.class, lockKey, "'lockKey' must of String type");
		return this.locks.computeIfAbsent((String) lockKey, DynamoDbLock::new);
	}

	@Override
	public void expireUnusedOlderThan(long age) {
		Iterator<Map.Entry<String, DynamoDbLock>> iterator = this.locks.entrySet().iterator();
		long now = System.currentTimeMillis();
		while (iterator.hasNext()) {
			Map.Entry<String, DynamoDbLock> entry = iterator.next();
			DynamoDbLock lock = entry.getValue();
			if (now - lock.lastUsed > age && !lock.delegate.isHeldByCurrentThread()) {
				iterator.remove();
			}
		}
	}

	@Override
	public String toString() {
		return "DynamoDbLockRegistry{" + "tableName='" + this.tableName + '\'' + ", billingMode=" + this.billingMode
				+ ", readCapacity=" + this.readCapacity + ", writeCapacity=" + this.writeCapacity + ", partitionKey='"
				+ this.partitionKey + '\'' + ", sortKeyName='" + this.sortKeyName + '\'' + ", sortKey='" + this.sortKey
				+ '\'' + ", refreshPeriod=" + this.refreshPeriod + ", leaseDuration=" + this.leaseDuration
				+ ", heartbeatPeriod=" + this.heartbeatPeriod + '}';
	}

	private final class DynamoDbLock implements Lock {

		private final ReentrantLock delegate = new ReentrantLock();

		private final String key;

		// It is safe to use a shared instance - access is guaranteed by the delegate
		// lock.
		private final AcquireLockOptions.AcquireLockOptionsBuilder acquireLockOptionsBuilder;

		private LockItem lockItem;

		private volatile long lastUsed = System.currentTimeMillis();

		private DynamoDbLock(String key) {
			this.key = key;
			this.acquireLockOptionsBuilder = AcquireLockOptions.builder(this.key).withReplaceData(false)
					.withSortKey(DynamoDbLockRegistry.this.sortKey).withTimeUnit(TimeUnit.MILLISECONDS);
		}

		private void rethrowAsLockException(Exception e) {
			throw new CannotAcquireLockException("Failed to lock at " + this.key, e);
		}

		@Override
		public void lock() {
			awaitForActive();

			this.delegate.lock();

			setupDefaultAcquireLockOptionsBuilder();

			boolean wasInterruptedWhileUninterruptible = false;

			try {
				while (true) {
					try {
						while (!doLock()) {
							Thread.sleep(100); // NOSONAR
						}
						break;
					}
					catch (InterruptedException e) {
						/*
						 * This method must be uninterruptible so catch and ignore
						 * interrupts and only break out of the while loop when we get the
						 * lock.
						 */
						wasInterruptedWhileUninterruptible = true;
					}
					catch (Exception e) {
						this.delegate.unlock();
						rethrowAsLockException(e);
					}
				}
			}
			finally {
				if (wasInterruptedWhileUninterruptible) {
					Thread.currentThread().interrupt();
				}
			}

		}

		private void setupDefaultAcquireLockOptionsBuilder() {
			this.acquireLockOptionsBuilder
					.withAdditionalTimeToWaitForLock(Long.MAX_VALUE - DynamoDbLockRegistry.this.leaseDuration)
					.withRefreshPeriod(DynamoDbLockRegistry.this.refreshPeriod);
		}

		@Override
		public void lockInterruptibly() throws InterruptedException {
			awaitForActive();

			this.delegate.lockInterruptibly();

			setupDefaultAcquireLockOptionsBuilder();

			try {
				while (!doLock()) {
					Thread.sleep(100); // NOSONAR
					if (Thread.currentThread().isInterrupted()) {
						throw new InterruptedException();
					}
				}
			}
			catch (InterruptedException ie) {
				this.delegate.unlock();
				Thread.currentThread().interrupt();
				throw ie;
			}
			catch (Exception e) {
				this.delegate.unlock();
				rethrowAsLockException(e);
			}
		}

		@Override
		public boolean tryLock() {
			try {
				return tryLock(0, TimeUnit.MILLISECONDS);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return false;
			}
		}

		@Override
		public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
			long start = System.currentTimeMillis();

			awaitForActive();

			if (!this.delegate.tryLock(time, unit)) {
				return false;
			}

			long additionalTimeToWait =
					TimeUnit.MILLISECONDS.convert(time, unit)
							- System.currentTimeMillis() + start - DynamoDbLockRegistry.this.leaseDuration;

			this.acquireLockOptionsBuilder.withAdditionalTimeToWaitForLock(additionalTimeToWait);

			boolean acquired = false;
			try {
				acquired = doLock();

				if (!acquired) {
					this.delegate.unlock();
				}
				else {
					this.lastUsed = System.currentTimeMillis();
				}
			}
			catch (LockCurrentlyUnavailableException ex) {
				this.delegate.unlock();
				logger.trace("The lock '" + this + "' cannot be acquired at the moment", ex);
			}
			catch (Exception e) {
				this.delegate.unlock();
				rethrowAsLockException(e);
			}

			return acquired;
		}

		private boolean doLock() throws InterruptedException {
			boolean acquired = false;
			if (this.lockItem != null) {
				try {
					this.lockItem.sendHeartBeat();
					acquired = true;
				}
				catch (LockNotGrantedException ex) {
					// May be no lock record in the DB - discard local holder and try to lock again
					this.lockItem = null;
				}
			}

			if (this.lockItem == null) {
				this.lockItem = DynamoDbLockRegistry.this.dynamoDBLockClient
						.tryAcquireLock(this.acquireLockOptionsBuilder.build()).orElse(null);

				acquired = this.lockItem != null;
			}

			if (acquired) {
				this.lastUsed = System.currentTimeMillis();
			}

			return acquired;
		}

		@Override
		public void unlock() {
			if (!this.delegate.isHeldByCurrentThread()) {
				throw new IllegalMonitorStateException("You do not own lock at " + this.key);
			}
			if (this.delegate.getHoldCount() > 1) {
				this.delegate.unlock();
				return;
			}
			try {
				if (Thread.currentThread().isInterrupted()) {
					LockItem lockItemToRelease = this.lockItem;
					DynamoDbLockRegistry.this.customizableThreadFactory
							.newThread(() ->
									DynamoDbLockRegistry.this.dynamoDBLockClient.releaseLock(lockItemToRelease))
							.start();
				}
				else {
					DynamoDbLockRegistry.this.dynamoDBLockClient.releaseLock(this.lockItem);
				}
			}
			catch (Exception e) {
				throw new DataAccessResourceFailureException("Failed to release lock at " + this.key, e);
			}
			finally {
				this.lockItem = null;
				this.delegate.unlock();
			}
		}

		@Override
		public Condition newCondition() {
			throw new UnsupportedOperationException("DynamoDb locks don't support conditions.");
		}

		@Override
		public String toString() {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd@HH:mm:ss.SSS");
			return "DynamoDbLock [lockKey=" + this.key + ",lockedAt=" + dateFormat.format(new Date(this.lastUsed))
					+ ", lockItem=" + this.lockItem + "]";
		}

	}

}
