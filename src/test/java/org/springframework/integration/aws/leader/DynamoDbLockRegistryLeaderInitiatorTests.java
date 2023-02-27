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

package org.springframework.integration.aws.leader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.waiters.FixedDelayStrategy;
import com.amazonaws.waiters.MaxAttemptsRetryStrategy;
import com.amazonaws.waiters.PollingStrategy;
import com.amazonaws.waiters.Waiter;
import com.amazonaws.waiters.WaiterParameters;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.integration.aws.LocalstackContainerTest;
import org.springframework.integration.aws.lock.DynamoDbLockRegistry;
import org.springframework.integration.aws.lock.DynamoDbLockRepository;
import org.springframework.integration.leader.Context;
import org.springframework.integration.leader.DefaultCandidate;
import org.springframework.integration.leader.event.LeaderEventPublisher;
import org.springframework.integration.support.leader.LockRegistryLeaderInitiator;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Artem Bilan
 *
 * @since 2.0
 */
class DynamoDbLockRegistryLeaderInitiatorTests implements LocalstackContainerTest {

	private static AmazonDynamoDBAsync DYNAMO_DB;

	@BeforeAll
	static void init() {
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

	@AfterAll
	static void destroy() {
		DYNAMO_DB.deleteTable(DynamoDbLockRepository.DEFAULT_TABLE_NAME);
	}

	@Test
	void testDistributedLeaderElection() throws Exception {
		CountDownLatch granted = new CountDownLatch(1);
		CountingPublisher countingPublisher = new CountingPublisher(granted);
		List<DynamoDbLockRepository> repositories = new ArrayList<>();
		List<LockRegistryLeaderInitiator> initiators = new ArrayList<>();
		for (int i = 0; i < 2; i++) {
			DynamoDbLockRepository dynamoDbLockRepository = new DynamoDbLockRepository(DYNAMO_DB);
			dynamoDbLockRepository.setLeaseDuration(Duration.ofSeconds(1));
			dynamoDbLockRepository.afterPropertiesSet();
			repositories.add(dynamoDbLockRepository);
			DynamoDbLockRegistry lockRepository = new DynamoDbLockRegistry(dynamoDbLockRepository);

			LockRegistryLeaderInitiator initiator = new LockRegistryLeaderInitiator(lockRepository,
					new DefaultCandidate("foo#" + i, "bar"));
			initiator.setExecutorService(
					Executors.newSingleThreadExecutor(new CustomizableThreadFactory("lock-leadership-" + i + "-")));
			initiator.setLeaderEventPublisher(countingPublisher);
			initiators.add(initiator);
		}

		for (LockRegistryLeaderInitiator initiator : initiators) {
			initiator.start();
		}

		assertThat(granted.await(20, TimeUnit.SECONDS)).isTrue();

		LockRegistryLeaderInitiator initiator1 = countingPublisher.initiator;

		LockRegistryLeaderInitiator initiator2 = null;

		for (LockRegistryLeaderInitiator initiator : initiators) {
			if (initiator != initiator1) {
				initiator2 = initiator;
				break;
			}
		}

		assertThat(initiator2).isNotNull();

		assertThat(initiator1.getContext().isLeader()).isTrue();
		assertThat(initiator2.getContext().isLeader()).isFalse();

		final CountDownLatch granted1 = new CountDownLatch(1);
		final CountDownLatch granted2 = new CountDownLatch(1);
		CountDownLatch revoked1 = new CountDownLatch(1);
		CountDownLatch revoked2 = new CountDownLatch(1);
		CountDownLatch acquireLockFailed1 = new CountDownLatch(1);
		CountDownLatch acquireLockFailed2 = new CountDownLatch(1);

		initiator1.setLeaderEventPublisher(new CountingPublisher(granted1, revoked1, acquireLockFailed1));

		initiator2.setLeaderEventPublisher(new CountingPublisher(granted2, revoked2, acquireLockFailed2));

		// It's hard to see round-robin election, so let's make the yielding initiator to
		// sleep long before restarting
		initiator1.setBusyWaitMillis(10000);

		initiator1.getContext().yield();

		assertThat(revoked1.await(20, TimeUnit.SECONDS)).isTrue();
		assertThat(granted2.await(20, TimeUnit.SECONDS)).isTrue();

		assertThat(initiator2.getContext().isLeader()).isTrue();
		assertThat(initiator1.getContext().isLeader()).isFalse();

		initiator1.setBusyWaitMillis(LockRegistryLeaderInitiator.DEFAULT_BUSY_WAIT_TIME);
		initiator2.setBusyWaitMillis(10000);

		initiator2.getContext().yield();

		assertThat(revoked2.await(20, TimeUnit.SECONDS)).isTrue();
		assertThat(granted1.await(20, TimeUnit.SECONDS)).isTrue();

		assertThat(initiator1.getContext().isLeader()).isTrue();
		assertThat(initiator2.getContext().isLeader()).isFalse();

		initiator2.stop();

		CountDownLatch revoked11 = new CountDownLatch(1);
		initiator1.setLeaderEventPublisher(
				new CountingPublisher(new CountDownLatch(1), revoked11, new CountDownLatch(1)));

		initiator1.getContext().yield();

		assertThat(revoked11.await(20, TimeUnit.SECONDS)).isTrue();
		assertThat(initiator1.getContext().isLeader()).isFalse();

		initiator1.stop();

		for (DynamoDbLockRepository dynamoDbLockRepository : repositories) {
			dynamoDbLockRepository.close();
		}
	}

	@Test
	void testLostConnection() throws Exception {
		CountDownLatch granted = new CountDownLatch(1);
		CountingPublisher countingPublisher = new CountingPublisher(granted);

		DynamoDbLockRepository dynamoDbLockRepository = new DynamoDbLockRepository(DYNAMO_DB);
		dynamoDbLockRepository.afterPropertiesSet();
		DynamoDbLockRegistry lockRepository = new DynamoDbLockRegistry(dynamoDbLockRepository);

		LockRegistryLeaderInitiator initiator = new LockRegistryLeaderInitiator(lockRepository);
		initiator.setLeaderEventPublisher(countingPublisher);

		initiator.start();

		assertThat(granted.await(20, TimeUnit.SECONDS)).isTrue();

		destroy();

		assertThat(countingPublisher.revoked.await(20, TimeUnit.SECONDS)).isTrue();

		granted = new CountDownLatch(1);
		countingPublisher = new CountingPublisher(granted);
		initiator.setLeaderEventPublisher(countingPublisher);

		init();

		dynamoDbLockRepository.afterPropertiesSet();

		assertThat(granted.await(20, TimeUnit.SECONDS)).isTrue();

		initiator.stop();

		dynamoDbLockRepository.close();
	}

	private static class CountingPublisher implements LeaderEventPublisher {

		private final CountDownLatch granted;

		private final CountDownLatch revoked;

		private final CountDownLatch acquireLockFailed;

		private volatile LockRegistryLeaderInitiator initiator;

		CountingPublisher(CountDownLatch granted, CountDownLatch revoked, CountDownLatch acquireLockFailed) {
			this.granted = granted;
			this.revoked = revoked;
			this.acquireLockFailed = acquireLockFailed;
		}

		CountingPublisher(CountDownLatch granted) {
			this(granted, new CountDownLatch(1), new CountDownLatch(1));
		}

		@Override
		public void publishOnRevoked(Object source, Context context, String role) {
			this.revoked.countDown();
		}

		@Override
		public void publishOnFailedToAcquire(Object source, Context context, String role) {
			this.acquireLockFailed.countDown();
		}

		@Override
		public void publishOnGranted(Object source, Context context, String role) {
			this.initiator = (LockRegistryLeaderInitiator) source;
			this.granted.countDown();
		}

	}

}
