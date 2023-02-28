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
import java.time.Duration;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


import com.amazonaws.services.dynamodbv2.model.TransactionConflictException;

import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.integration.support.locks.ExpirableLockRegistry;
import org.springframework.integration.support.locks.RenewableLockRegistry;
import org.springframework.util.Assert;

/**
 * An {@link ExpirableLockRegistry} and {@link RenewableLockRegistry} implementation for the AWS DynamoDB.
 * The algorithm is based on the {@link DynamoDbLockRepository}.
 *
 * @author Artem Bilan
 * @author Karl Lessard
 * @author Asiel Caballero
 *
 * @since 2.0
 */
public class DynamoDbLockRegistry implements ExpirableLockRegistry, RenewableLockRegistry {

	private static final int DEFAULT_IDLE = 100;

	private final Map<String, DynamoDbLock> locks = new ConcurrentHashMap<>();

	private final DynamoDbLockRepository dynamoDbLockRepository;

	private Duration idleBetweenTries = Duration.ofMillis(DEFAULT_IDLE);

	public DynamoDbLockRegistry(DynamoDbLockRepository dynamoDbLockRepository) {
		Assert.notNull(dynamoDbLockRepository, "'dynamoDbLockRepository' must not be null");
		this.dynamoDbLockRepository = dynamoDbLockRepository;
	}


	/**
	 * Specify a {@link Duration} to sleep between lock record insert/update attempts.
	 * Defaults to 100 milliseconds.
	 * @param idleBetweenTries the {@link Duration} to sleep between insert/update attempts.
	 * @since 3.0
	 */
	public void setIdleBetweenTries(Duration idleBetweenTries) {
		Assert.notNull(idleBetweenTries, "'idleBetweenTries' must not be null");
		this.idleBetweenTries = idleBetweenTries;
	}

	@Override
	public Lock obtain(Object lockKey) {
		Assert.isInstanceOf(String.class, lockKey, "'lockKey' must of String type");
		return this.locks.computeIfAbsent((String) lockKey, DynamoDbLock::new);
	}

	@Override
	public void expireUnusedOlderThan(long age) {
		long now = System.currentTimeMillis();
		synchronized (this.locks) {
			this.locks.entrySet()
					.removeIf(entry -> {
						DynamoDbLock lock = entry.getValue();
						return now - lock.lastUsed > age && !lock.isAcquiredInThisProcess();
					});
		}
	}

	@Override
	public void renewLock(Object lockKey) {
		Assert.isInstanceOf(String.class, lockKey, "'lockKey' must of String type");
		String lockId = (String) lockKey;
		DynamoDbLock dynamoDbLock = this.locks.get(lockId);
		if (dynamoDbLock == null) {
			throw new IllegalStateException("Could not found mutex at " + lockId);
		}
		if (!dynamoDbLock.renew()) {
			throw new IllegalStateException("Could not renew mutex at " + lockId);
		}
	}

	@Override
	public String toString() {
		return "DynamoDbLockRegistry{" + "tableName='" + this.dynamoDbLockRepository.getTableName() + '\''
				+ ", owner='" + this.dynamoDbLockRepository.getOwner() + '}';
	}

	private final class DynamoDbLock implements Lock {

		private final ReentrantLock delegate = new ReentrantLock();

		private final String key;

		private volatile long lastUsed = System.currentTimeMillis();

		private DynamoDbLock(String key) {
			this.key = key;
		}

		private void rethrowAsLockException(Exception e) {
			throw new CannotAcquireLockException("Failed to lock at " + this.key, e);
		}

		@Override
		public void lock() {
			this.delegate.lock();
			while (true) {
				try {
					while (!doLock()) {
						sleepBetweenRetries();
					}
					break;
				}
				catch (TransactionConflictException ex) {
					// try again
				}
				catch (InterruptedException ex) {
					/*
					 * This method must be uninterruptible so catch and ignore
					 * interrupts and only break out of the while loop when
					 * we get the lock.
					 */
				}
				catch (Exception ex) {
					this.delegate.unlock();
					rethrowAsLockException(ex);
				}
			}

		}

		@Override
		public void lockInterruptibly() throws InterruptedException {
			this.delegate.lockInterruptibly();
			while (true) {
				try {
					while (!doLock()) {
						sleepBetweenRetries();
						if (Thread.currentThread().isInterrupted()) {
							throw new InterruptedException();
						}
					}
					break;
				}
				catch (TransactionConflictException ex) {
					// try again
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
		}

		@Override
		public boolean tryLock() {
			try {
				return tryLock(0, TimeUnit.MILLISECONDS);
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				return false;
			}
		}

		@Override
		public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
			long now = System.currentTimeMillis();
			if (!this.delegate.tryLock(time, unit)) {
				return false;
			}
			long expire = now + TimeUnit.MILLISECONDS.convert(time, unit);
			boolean acquired;
			while (true) {
				try {
					while (!(acquired = doLock()) && System.currentTimeMillis() < expire) { //NOSONAR
						sleepBetweenRetries();
					}
					if (!acquired) {
						this.delegate.unlock();
					}
					return acquired;
				}
				catch (TransactionConflictException ex) {
					// try again
				}
				catch (Exception ex) {
					this.delegate.unlock();
					rethrowAsLockException(ex);
				}
			}
		}

		private boolean doLock() throws InterruptedException {
			boolean acquired = DynamoDbLockRegistry.this.dynamoDbLockRepository.acquire(this.key);
			if (acquired) {
				this.lastUsed = System.currentTimeMillis();
			}
			return acquired;
		}

		@Override
		public void unlock() {
			if (!this.delegate.isHeldByCurrentThread()) {
				throw new IllegalMonitorStateException("The current thread doesn't own mutex at '" + this.key + "'");
			}
			if (this.delegate.getHoldCount() > 1) {
				this.delegate.unlock();
				return;
			}
			try {
				while (true) {
					try {
						DynamoDbLockRegistry.this.dynamoDbLockRepository.delete(this.key);
						return;
					}
					catch (TransactionConflictException ex) {
						// try again
						try {
							sleepBetweenRetries();
						}
						catch (InterruptedException intEx) {
							/*
							 * This method must be uninterruptible so catch and ignore
							 * interrupts and only break out of the while loop when
							 * we get 'renewed' result.
							 */
						}
					}
					catch (Exception ex) {
						throw new DataAccessResourceFailureException("Failed to release mutex at " + this.key, ex);
					}
				}
			}
			finally {
				this.delegate.unlock();
			}
		}

		public boolean renew() {
			if (!this.delegate.isHeldByCurrentThread()) {
				throw new IllegalMonitorStateException("The current thread doesn't own mutex at " + this.key);
			}
			while (true) {
				try {
					boolean renewed = DynamoDbLockRegistry.this.dynamoDbLockRepository.renew(this.key);
					if (renewed) {
						this.lastUsed = System.currentTimeMillis();
					}
					return renewed;
				}
				catch (TransactionConflictException ex) {
					// try again
					try {
						sleepBetweenRetries();
					}
					catch (InterruptedException intEx) {
						/*
						 * This method must be uninterruptible so catch and ignore
						 * interrupts and only break out of the while loop when
						 * we get 'renewed' result.
						 */
					}
				}
				catch (Exception ex) {
					throw new DataAccessResourceFailureException("Failed to renew mutex at " + this.key, ex);
				}
			}
		}

		public boolean isAcquiredInThisProcess() {
			return DynamoDbLockRegistry.this.dynamoDbLockRepository.isAcquired(this.key);
		}

		private void sleepBetweenRetries() throws InterruptedException {
			Thread.sleep(DynamoDbLockRegistry.this.idleBetweenTries.toMillis());
		}

		@Override
		public Condition newCondition() {
			throw new UnsupportedOperationException("DynamoDb locks don't support conditions.");
		}

		@Override
		public String toString() {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd@HH:mm:ss.SSS");
			return "DynamoDbLock [lockKey=" + this.key + ",lockedAt=" + dateFormat.format(new Date(this.lastUsed)) + "]";
		}

	}

}
