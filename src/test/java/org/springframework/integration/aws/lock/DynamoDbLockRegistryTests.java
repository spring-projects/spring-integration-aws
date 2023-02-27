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

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.waiters.FixedDelayStrategy;
import com.amazonaws.waiters.MaxAttemptsRetryStrategy;
import com.amazonaws.waiters.PollingStrategy;
import com.amazonaws.waiters.Waiter;
import com.amazonaws.waiters.WaiterParameters;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.integration.aws.LocalstackContainerTest;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * @author Artem Bilan
 *
 * @since 2.0
 */
@SpringJUnitConfig
@DirtiesContext
public class DynamoDbLockRegistryTests implements LocalstackContainerTest {

	private static AmazonDynamoDBAsync DYNAMO_DB;

	private final AsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();

	@Autowired
	private DynamoDbLockRepository dynamoDbLockRepository;

	@Autowired
	private DynamoDbLockRegistry dynamoDbLockRegistry;

	@BeforeAll
	static void setup() {
		DYNAMO_DB = LocalstackContainerTest.dynamoDbClient();
		try {
			DYNAMO_DB.deleteTableAsync(DynamoDbLockRepository.DEFAULT_TABLE_NAME);

			Waiter<DescribeTableRequest> waiter = DYNAMO_DB.waiters().tableNotExists();

			waiter.run(new WaiterParameters<>(new DescribeTableRequest(DynamoDbLockRepository.DEFAULT_TABLE_NAME))
					.withPollingStrategy(
							new PollingStrategy(new MaxAttemptsRetryStrategy(25), new FixedDelayStrategy(1))));
		}
		catch (Exception e) {
			// Ignore
		}
	}

	@BeforeEach
	void clear() {
		this.dynamoDbLockRepository.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	void testLock() {
		for (int i = 0; i < 10; i++) {
			Lock lock = this.dynamoDbLockRegistry.obtain("foo");
			lock.lock();
			try {
				assertThat(TestUtils.getPropertyValue(this.dynamoDbLockRepository, "heldLocks", Set.class)).hasSize(1);
			}
			finally {
				lock.unlock();
			}
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void testLockInterruptibly() throws Exception {
		for (int i = 0; i < 10; i++) {
			Lock lock = this.dynamoDbLockRegistry.obtain("foo");
			lock.lockInterruptibly();
			try {
				assertThat(TestUtils.getPropertyValue(this.dynamoDbLockRepository, "heldLocks", Set.class)).hasSize(1);
			}
			finally {
				lock.unlock();
			}
		}
	}

	@Test
	void testReentrantLock() {
		for (int i = 0; i < 10; i++) {
			Lock lock1 = this.dynamoDbLockRegistry.obtain("foo");
			lock1.lock();
			try {
				Lock lock2 = this.dynamoDbLockRegistry.obtain("foo");
				assertThat(lock1).isSameAs(lock2);
				lock2.lock();
				lock2.unlock();
			}
			finally {
				lock1.unlock();
			}
		}
	}

	@Test
	void testReentrantLockInterruptibly() throws Exception {
		for (int i = 0; i < 10; i++) {
			Lock lock1 = this.dynamoDbLockRegistry.obtain("foo");
			lock1.lockInterruptibly();
			try {
				Lock lock2 = this.dynamoDbLockRegistry.obtain("foo");
				assertThat(lock1).isSameAs(lock2);
				lock2.lockInterruptibly();
				lock2.unlock();
			}
			finally {
				lock1.unlock();
			}
		}
	}

	@Test
	void testTwoLocks() throws Exception {
		for (int i = 0; i < 10; i++) {
			Lock lock1 = this.dynamoDbLockRegistry.obtain("foo");
			lock1.lockInterruptibly();
			try {
				Lock lock2 = this.dynamoDbLockRegistry.obtain("bar");
				assertThat(lock1).isNotSameAs(lock2);
				lock2.lockInterruptibly();
				lock2.unlock();
			}
			finally {
				lock1.unlock();
			}
		}
	}

	@Test
	void testTwoThreadsSecondFailsToGetLock() throws Exception {
		final Lock lock1 = this.dynamoDbLockRegistry.obtain("foo");
		lock1.lockInterruptibly();
		final AtomicBoolean locked = new AtomicBoolean();
		final CountDownLatch latch = new CountDownLatch(1);
		Future<Object> result = this.taskExecutor.submit(() -> {
			DynamoDbLockRepository dynamoDbLockRepository = new DynamoDbLockRepository(DYNAMO_DB);
			dynamoDbLockRepository.setLeaseDuration(Duration.ofSeconds(10));
			dynamoDbLockRepository.afterPropertiesSet();
			DynamoDbLockRegistry registry2 = new DynamoDbLockRegistry(dynamoDbLockRepository);
			registry2.setIdleBetweenTries(Duration.ofMillis(10));
			Lock lock2 = registry2.obtain("foo");
			locked.set(lock2.tryLock());
			latch.countDown();
			try {
				lock2.unlock();
			}
			catch (Exception e) {
				return e;
			}
			finally {
				dynamoDbLockRepository.close();
			}
			return null;
		});
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(locked.get()).isFalse();
		lock1.unlock();
		Object ise = result.get(10, TimeUnit.SECONDS);
		assertThat(ise).isInstanceOf(IllegalMonitorStateException.class);
		assertThat(((Exception) ise).getMessage()).contains("The current thread doesn't own mutex at 'foo'");
	}

	@Test
	void testTwoThreads() throws Exception {
		final Lock lock1 = this.dynamoDbLockRegistry.obtain("foo");
		final AtomicBoolean locked = new AtomicBoolean();
		final CountDownLatch latch1 = new CountDownLatch(1);
		final CountDownLatch latch2 = new CountDownLatch(1);
		final CountDownLatch latch3 = new CountDownLatch(1);
		lock1.lockInterruptibly();
		this.taskExecutor.submit(() -> {
			DynamoDbLockRepository dynamoDbLockRepository = new DynamoDbLockRepository(DYNAMO_DB);
			dynamoDbLockRepository.setLeaseDuration(Duration.ofSeconds(10));
			dynamoDbLockRepository.afterPropertiesSet();
			DynamoDbLockRegistry registry2 = new DynamoDbLockRegistry(dynamoDbLockRepository);
			registry2.setIdleBetweenTries(Duration.ofMillis(10));
			Lock lock2 = registry2.obtain("foo");
			try {
				latch1.countDown();
				lock2.lockInterruptibly();
				latch2.await(10, TimeUnit.SECONDS);
				locked.set(true);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			finally {
				lock2.unlock();
				latch3.countDown();
				dynamoDbLockRepository.close();
			}
			return null;
		});

		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(locked.get()).isFalse();

		lock1.unlock();
		latch2.countDown();

		assertThat(latch3.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(locked.get()).isTrue();
	}

	@Test
	void testTwoThreadsDifferentRegistries() throws Exception {
		DynamoDbLockRepository dynamoDbLockRepository = new DynamoDbLockRepository(DYNAMO_DB);
		dynamoDbLockRepository.setLeaseDuration(Duration.ofSeconds(10));
		dynamoDbLockRepository.afterPropertiesSet();
		final DynamoDbLockRegistry registry1 = new DynamoDbLockRegistry(dynamoDbLockRepository);
		registry1.setIdleBetweenTries(Duration.ofMillis(10));

		final DynamoDbLockRegistry registry2 = new DynamoDbLockRegistry(dynamoDbLockRepository);
		registry2.setIdleBetweenTries(Duration.ofMillis(10));

		final Lock lock1 = registry1.obtain("foo");
		final AtomicBoolean locked = new AtomicBoolean();
		final CountDownLatch latch1 = new CountDownLatch(1);
		final CountDownLatch latch2 = new CountDownLatch(1);
		final CountDownLatch latch3 = new CountDownLatch(1);
		lock1.lockInterruptibly();
		this.taskExecutor.execute(() -> {
			Lock lock2 = registry2.obtain("foo");
			try {
				latch1.countDown();
				lock2.lockInterruptibly();
				latch2.await(10, TimeUnit.SECONDS);
				locked.set(true);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			finally {
				lock2.unlock();
				latch3.countDown();
			}
		});
		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(locked.get()).isFalse();

		lock1.unlock();
		latch2.countDown();

		assertThat(latch3.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(locked.get()).isTrue();

		dynamoDbLockRepository.close();
	}

	@Test
	void testTwoThreadsWrongOneUnlocks() throws Exception {
		final Lock lock = this.dynamoDbLockRegistry.obtain("foo");
		lock.lockInterruptibly();
		final AtomicBoolean locked = new AtomicBoolean();
		final CountDownLatch latch = new CountDownLatch(1);
		Future<Object> result = this.taskExecutor.submit(() -> {
			try {
				lock.unlock();
			}
			catch (Exception e) {
				latch.countDown();
				return e;
			}
			return null;
		});

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(locked.get()).isFalse();

		lock.unlock();
		Object imse = result.get(10, TimeUnit.SECONDS);
		assertThat(imse).isInstanceOf(IllegalMonitorStateException.class);
		assertThat(((Exception) imse).getMessage()).contains("The current thread doesn't own mutex at 'foo'");
	}

	@Test
	void abandonedLock() throws Exception {
		DynamoDbLockRepository dynamoDbLockRepository = new DynamoDbLockRepository(DYNAMO_DB);
		dynamoDbLockRepository.setLeaseDuration(Duration.ofSeconds(10));
		dynamoDbLockRepository.afterPropertiesSet();
		this.dynamoDbLockRepository.acquire("foo");

		Lock lock = this.dynamoDbLockRegistry.obtain("foo");
		int n = 0;
		while (!lock.tryLock() && n++ < 100) {
			Thread.sleep(100);
		}

		assertThat(n).isLessThan(100);
		lock.unlock();
		dynamoDbLockRepository.close();
	}

	@Test
	public void testLockRenew() {
		final Lock lock = this.dynamoDbLockRegistry.obtain("foo");

		assertThat(lock.tryLock()).isTrue();
		try {
			assertThatNoException().isThrownBy(() -> this.dynamoDbLockRegistry.renewLock("foo"));
		}
		finally {
			lock.unlock();
		}
	}

	@Configuration
	public static class ContextConfiguration {

		@Bean
		public DynamoDbLockRepository dynamoDbLockRepository() {
			DynamoDbLockRepository dynamoDbLockRepository = new DynamoDbLockRepository(DYNAMO_DB);
			dynamoDbLockRepository.setLeaseDuration(Duration.ofSeconds(2));
			return dynamoDbLockRepository;
		}

		@Bean
		public DynamoDbLockRegistry dynamoDbLockRegistry() {
			DynamoDbLockRegistry dynamoDbLockRegistry = new DynamoDbLockRegistry(dynamoDbLockRepository());
			dynamoDbLockRegistry.setIdleBetweenTries(Duration.ofMillis(10));
			return dynamoDbLockRegistry;
		}

	}

}
