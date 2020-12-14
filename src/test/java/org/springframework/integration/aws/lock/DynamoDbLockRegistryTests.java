/*
 * Copyright 2018-2020 the original author or authors.
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

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.integration.aws.EnvironmentHostNameResolver;
import org.springframework.integration.aws.ExtendedDockerTestUtils;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import cloud.localstack.docker.LocalstackDockerExtension;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.waiters.FixedDelayStrategy;
import com.amazonaws.waiters.MaxAttemptsRetryStrategy;
import com.amazonaws.waiters.PollingStrategy;
import com.amazonaws.waiters.Waiter;
import com.amazonaws.waiters.WaiterParameters;

/**
 * @author Artem Bilan
 *
 * @since 2.0
 */
@SpringJUnitConfig
@EnabledIfEnvironmentVariable(named = EnvironmentHostNameResolver.DOCKER_HOST_NAME, matches = ".+")
@ExtendWith(LocalstackDockerExtension.class)
@LocalstackDockerProperties(
		hostNameResolver = EnvironmentHostNameResolver.class,
		services = "dynamodb")
@DirtiesContext
public class DynamoDbLockRegistryTests {

	private static AmazonDynamoDBAsync DYNAMO_DB;

	private final AsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();

	@Autowired
	private DynamoDbLockRegistry dynamoDbLockRegistry;

	@BeforeAll
	static void setup() {
		DYNAMO_DB = ExtendedDockerTestUtils.getClientDynamoDbAsync();

		try {
			DYNAMO_DB.deleteTableAsync(DynamoDbLockRegistry.DEFAULT_TABLE_NAME);

			Waiter<DescribeTableRequest> waiter = DYNAMO_DB.waiters().tableNotExists();

			waiter.run(new WaiterParameters<>(new DescribeTableRequest(DynamoDbLockRegistry.DEFAULT_TABLE_NAME))
					.withPollingStrategy(
							new PollingStrategy(new MaxAttemptsRetryStrategy(25), new FixedDelayStrategy(1))));
		}
		catch (Exception e) {
			// Ignore
		}
	}

	@BeforeEach
	void clear() {
		this.dynamoDbLockRegistry.expireUnusedOlderThan(0);
	}

	@Test
	@SuppressWarnings("unchecked")
	void testLock() {
		for (int i = 0; i < 10; i++) {
			Lock lock = this.dynamoDbLockRegistry.obtain("foo");
			lock.lock();
			try {
				assertThat(TestUtils.getPropertyValue(this.dynamoDbLockRegistry, "locks", Map.class)).hasSize(1);
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
				assertThat(TestUtils.getPropertyValue(this.dynamoDbLockRegistry, "locks", Map.class)).hasSize(1);
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
			DynamoDbLockRegistry registry2 = new DynamoDbLockRegistry(DYNAMO_DB);
			registry2.setHeartbeatPeriod(1);
			registry2.setRefreshPeriod(10);
			registry2.setLeaseDuration(2);
			registry2.afterPropertiesSet();
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
				registry2.destroy();
			}
			return null;
		});
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(locked.get()).isFalse();
		lock1.unlock();
		Object ise = result.get(10, TimeUnit.SECONDS);
		assertThat(ise).isInstanceOf(IllegalMonitorStateException.class);
		assertThat(((Exception) ise).getMessage()).contains("You do not own");
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
			DynamoDbLockRegistry registry2 = new DynamoDbLockRegistry(DYNAMO_DB);
			registry2.setHeartbeatPeriod(1);
			registry2.setRefreshPeriod(10);
			registry2.setLeaseDuration(2);
			registry2.afterPropertiesSet();
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
				registry2.destroy();
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
		final DynamoDbLockRegistry registry1 = new DynamoDbLockRegistry(DYNAMO_DB);
		registry1.setHeartbeatPeriod(1);
		registry1.setRefreshPeriod(10);
		registry1.setLeaseDuration(2);
		registry1.afterPropertiesSet();

		final DynamoDbLockRegistry registry2 = new DynamoDbLockRegistry(DYNAMO_DB);
		registry2.setHeartbeatPeriod(1);
		registry2.setRefreshPeriod(10);
		registry2.setLeaseDuration(2);
		registry2.afterPropertiesSet();

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

		registry1.destroy();
		registry2.destroy();
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
		assertThat(((Exception) imse).getMessage()).contains("You do not own");
	}

	@Configuration
	public static class ContextConfiguration {

		@Bean
		public DynamoDbLockRegistry dynamoDbLockRegistry() {
			DynamoDbLockRegistry dynamoDbLockRegistry = new DynamoDbLockRegistry(DYNAMO_DB);
			dynamoDbLockRegistry.setHeartbeatPeriod(1);
			dynamoDbLockRegistry.setRefreshPeriod(10);
			dynamoDbLockRegistry.setLeaseDuration(2);
			return dynamoDbLockRegistry;
		}

	}

}
