/*
 * Copyright 2015-2016 the original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.springframework.integration.aws.config.xml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;

import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.aws.core.env.ResourceIdResolver;
import org.springframework.cloud.aws.messaging.listener.SimpleMessageListenerContainer;
import org.springframework.cloud.aws.messaging.listener.SqsMessageDeletionPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.integration.aws.inbound.SqsMessageDrivenChannelAdapter;
import org.springframework.integration.channel.NullChannel;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.core.DestinationResolutionException;
import org.springframework.messaging.core.DestinationResolver;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.amazonaws.services.sqs.AmazonSQS;

/**
 * @author Artem Bilan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class SqsMessageDrivenChannelAdapterParserTests {

	@Autowired
	private AmazonSQS amazonSqs;

	@Autowired
	private ResourceIdResolver resourceIdResolver;

	@Autowired
	private DestinationResolver<?> destinationResolver;

	@Autowired
	private TaskExecutor taskExecutor;

	@Autowired
	private MessageChannel errorChannel;

	@Autowired
	private NullChannel nullChannel;

	@Autowired
	private SqsMessageDrivenChannelAdapter sqsMessageDrivenChannelAdapter;

	@Bean
	public DestinationResolver<?> destinationResolver() {
		DestinationResolver<?> destinationResolver = Mockito.mock(DestinationResolver.class);
		doThrow(DestinationResolutionException.class)
				.when(destinationResolver)
				.resolveDestination(anyString());
		return destinationResolver;
	}

	@Test
	public void testSqsMessageDrivenChannelAdapterParser() {
		SimpleMessageListenerContainer listenerContainer =
				TestUtils.getPropertyValue(this.sqsMessageDrivenChannelAdapter, "listenerContainer",
						SimpleMessageListenerContainer.class);
		assertSame(this.amazonSqs, TestUtils.getPropertyValue(listenerContainer, "amazonSqs"));
		assertSame(this.resourceIdResolver, TestUtils.getPropertyValue(listenerContainer, "resourceIdResolver"));
		assertSame(this.taskExecutor, TestUtils.getPropertyValue(listenerContainer, "taskExecutor"));
		assertSame(this.destinationResolver, TestUtils.getPropertyValue(listenerContainer, "destinationResolver"));
		assertFalse(listenerContainer.isRunning());
		assertEquals(5, TestUtils.getPropertyValue(listenerContainer, "maxNumberOfMessages"));
		assertEquals(200, TestUtils.getPropertyValue(listenerContainer, "visibilityTimeout"));
		assertEquals(40, TestUtils.getPropertyValue(listenerContainer, "waitTimeOut"));

		@SuppressWarnings("rawtypes")
		Map queues = TestUtils.getPropertyValue(listenerContainer, "registeredQueues", Map.class);
		assertTrue(queues.keySet().contains("foo"));
		assertTrue(queues.keySet().contains("bar"));

		assertEquals(100, this.sqsMessageDrivenChannelAdapter.getPhase());
		assertFalse(this.sqsMessageDrivenChannelAdapter.isAutoStartup());
		assertFalse(this.sqsMessageDrivenChannelAdapter.isRunning());
		assertSame(this.errorChannel, TestUtils.getPropertyValue(this.sqsMessageDrivenChannelAdapter, "outputChannel"));
		assertSame(this.nullChannel, TestUtils.getPropertyValue(this.sqsMessageDrivenChannelAdapter, "errorChannel"));
		assertEquals(2000L, TestUtils.getPropertyValue(this.sqsMessageDrivenChannelAdapter,
				"messagingTemplate.sendTimeout"));
		assertEquals(SqsMessageDeletionPolicy.NEVER,
				TestUtils.getPropertyValue(this.sqsMessageDrivenChannelAdapter, "messageDeletionPolicy",
						SqsMessageDeletionPolicy.class));
	}

}
