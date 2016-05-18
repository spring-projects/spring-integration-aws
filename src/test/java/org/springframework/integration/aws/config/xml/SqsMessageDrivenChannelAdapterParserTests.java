/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.aws.config.xml;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Matchers.anyString;

import java.util.Map;

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
		willThrow(DestinationResolutionException.class)
				.given(destinationResolver)
				.resolveDestination(anyString());
		return destinationResolver;
	}

	@Test
	public void testSqsMessageDrivenChannelAdapterParser() {
		SimpleMessageListenerContainer listenerContainer =
				TestUtils.getPropertyValue(this.sqsMessageDrivenChannelAdapter, "listenerContainer",
						SimpleMessageListenerContainer.class);
		assertThat(TestUtils.getPropertyValue(listenerContainer, "amazonSqs")).isSameAs(this.amazonSqs);
		assertThat(TestUtils.getPropertyValue(listenerContainer, "resourceIdResolver"))
				.isSameAs(this.resourceIdResolver);
		assertThat(TestUtils.getPropertyValue(listenerContainer, "taskExecutor")).isSameAs(this.taskExecutor);
		assertThat(TestUtils.getPropertyValue(listenerContainer, "destinationResolver"))
				.isSameAs(this.destinationResolver);
		assertThat(listenerContainer.isRunning()).isFalse();
		assertThat(TestUtils.getPropertyValue(listenerContainer, "maxNumberOfMessages")).isEqualTo(5);
		assertThat(TestUtils.getPropertyValue(listenerContainer, "visibilityTimeout")).isEqualTo(200);
		assertThat(TestUtils.getPropertyValue(listenerContainer, "waitTimeOut")).isEqualTo(40);

		@SuppressWarnings("rawtypes")
		Map queues = TestUtils.getPropertyValue(listenerContainer, "registeredQueues", Map.class);
		assertThat(queues.keySet().contains("foo")).isTrue();
		assertThat(queues.keySet().contains("bar")).isTrue();

		assertThat(this.sqsMessageDrivenChannelAdapter.getPhase()).isEqualTo(100);
		assertThat(this.sqsMessageDrivenChannelAdapter.isAutoStartup()).isFalse();
		assertThat(this.sqsMessageDrivenChannelAdapter.isRunning()).isFalse();
		assertThat(TestUtils.getPropertyValue(this.sqsMessageDrivenChannelAdapter, "outputChannel"))
				.isSameAs(this.errorChannel);
		assertThat(TestUtils.getPropertyValue(this.sqsMessageDrivenChannelAdapter, "errorChannel"))
				.isSameAs(this.nullChannel);
		assertThat(TestUtils.getPropertyValue(this.sqsMessageDrivenChannelAdapter,
				"messagingTemplate.sendTimeout"))
				.isEqualTo(2000L);
		assertThat(TestUtils.getPropertyValue(this.sqsMessageDrivenChannelAdapter, "messageDeletionPolicy",
				SqsMessageDeletionPolicy.class))
				.isEqualTo(SqsMessageDeletionPolicy.NEVER);
	}

}
