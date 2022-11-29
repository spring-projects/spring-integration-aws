/*
 * Copyright 2016-2022 the original author or authors.
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

package org.springframework.integration.aws.inbound;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.support.management.IntegrationManagedResource;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.core.DestinationResolver;
import org.springframework.messaging.handler.HandlerMethod;
import org.springframework.util.Assert;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import io.awspring.cloud.core.env.ResourceIdResolver;
import io.awspring.cloud.messaging.config.SimpleMessageListenerContainerFactory;
import io.awspring.cloud.messaging.listener.QueueMessageHandler;
import io.awspring.cloud.messaging.listener.SimpleMessageListenerContainer;
import io.awspring.cloud.messaging.listener.SqsMessageDeletionPolicy;

/**
 * The {@link MessageProducerSupport} implementation for the Amazon SQS
 * {@code receiveMessage}. Works in 'listener' manner and delegates hard to the
 * {@link SimpleMessageListenerContainer}.
 *
 * @author Artem Bilan
 * @author Patrick Fitzsimons
 *
 * @see SimpleMessageListenerContainerFactory
 * @see SimpleMessageListenerContainer
 * @see QueueMessageHandler
 */
@ManagedResource
@IntegrationManagedResource
public class SqsMessageDrivenChannelAdapter extends MessageProducerSupport implements DisposableBean {

	private final SimpleMessageListenerContainerFactory simpleMessageListenerContainerFactory =
			new SimpleMessageListenerContainerFactory();

	private final String[] queues;

	private SimpleMessageListenerContainer listenerContainer;

	private Long queueStopTimeout;

	private SqsMessageDeletionPolicy messageDeletionPolicy = SqsMessageDeletionPolicy.NO_REDRIVE;

	public SqsMessageDrivenChannelAdapter(AmazonSQSAsync amazonSqs, String... queues) {
		Assert.noNullElements(queues, "'queues' must not be empty");
		this.simpleMessageListenerContainerFactory.setAmazonSqs(amazonSqs);
		this.queues = Arrays.copyOf(queues, queues.length);
	}

	public void setTaskExecutor(AsyncTaskExecutor taskExecutor) {
		this.simpleMessageListenerContainerFactory.setTaskExecutor(taskExecutor);
	}

	public void setMaxNumberOfMessages(Integer maxNumberOfMessages) {
		this.simpleMessageListenerContainerFactory.setMaxNumberOfMessages(maxNumberOfMessages);
	}

	public void setVisibilityTimeout(Integer visibilityTimeout) {
		this.simpleMessageListenerContainerFactory.setVisibilityTimeout(visibilityTimeout);
	}

	public void setWaitTimeOut(Integer waitTimeOut) {
		this.simpleMessageListenerContainerFactory.setWaitTimeOut(waitTimeOut);
	}

	public void setResourceIdResolver(ResourceIdResolver resourceIdResolver) {
		this.simpleMessageListenerContainerFactory.setResourceIdResolver(resourceIdResolver);
	}

	@Override
	public void setAutoStartup(boolean autoStartUp) {
		super.setAutoStartup(autoStartUp);
		this.simpleMessageListenerContainerFactory.setAutoStartup(autoStartUp);
	}

	public void setDestinationResolver(DestinationResolver<String> destinationResolver) {
		this.simpleMessageListenerContainerFactory.setDestinationResolver(destinationResolver);
	}

	public void setFailOnMissingQueue(boolean failOnMissingQueue) {
		this.simpleMessageListenerContainerFactory.setFailOnMissingQueue(failOnMissingQueue);
	}

	public void setQueueStopTimeout(long queueStopTimeout) {
		this.queueStopTimeout = queueStopTimeout;
	}

	public void setMessageDeletionPolicy(SqsMessageDeletionPolicy messageDeletionPolicy) {
		Assert.notNull(messageDeletionPolicy, "'messageDeletionPolicy' must not be null.");
		this.messageDeletionPolicy = messageDeletionPolicy;
	}

	@Override
	protected void onInit() {
		super.onInit();
		this.listenerContainer = this.simpleMessageListenerContainerFactory.createSimpleMessageListenerContainer();
		if (this.queueStopTimeout != null) {
			this.listenerContainer.setQueueStopTimeout(this.queueStopTimeout);
		}
		this.listenerContainer.setMessageHandler(new IntegrationQueueMessageHandler());
		try {
			this.listenerContainer.afterPropertiesSet();
		}
		catch (Exception e) {
			throw new BeanCreationException("Cannot instantiate 'SimpleMessageListenerContainer'", e);
		}
	}

	@Override
	public String getComponentType() {
		return "aws:sqs-message-driven-channel-adapter";
	}

	@Override
	protected void doStart() {
		this.listenerContainer.start();
	}

	@Override
	protected void doStop() {
		this.listenerContainer.stop();
	}

	@ManagedOperation
	public void stop(String logicalQueueName) {
		this.listenerContainer.stop(logicalQueueName);
	}

	@ManagedOperation
	public void start(String logicalQueueName) {
		this.listenerContainer.start(logicalQueueName);
	}

	@ManagedOperation
	public boolean isRunning(String logicalQueueName) {
		return this.listenerContainer.isRunning(logicalQueueName);
	}

	@ManagedAttribute
	public String[] getQueues() {
		return Arrays.copyOf(this.queues, this.queues.length);
	}

	@Override
	public void destroy() {
		this.listenerContainer.destroy();
	}

	private class IntegrationQueueMessageHandler extends QueueMessageHandler {

		@Override
		public Map<MappingInformation, HandlerMethod> getHandlerMethods() {
			Set<String> queues = new HashSet<>(Arrays.asList(SqsMessageDrivenChannelAdapter.this.queues));
			MappingInformation mappingInformation = new MappingInformation(queues,
					SqsMessageDrivenChannelAdapter.this.messageDeletionPolicy);
			return Collections.singletonMap(mappingInformation, null);
		}

		@Override
		protected void handleMessageInternal(Message<?> message, String lookupDestination) {
			MessageHeaders headers = message.getHeaders();

			Message<?> messageToSend = getMessageBuilderFactory().fromMessage(message)
					.removeHeaders("LogicalResourceId", "MessageId", "ReceiptHandle", "Acknowledgment")
					.setHeader(AwsHeaders.MESSAGE_ID, headers.get("MessageId"))
					.setHeader(AwsHeaders.RECEIPT_HANDLE, headers.get("ReceiptHandle"))
					.setHeader(AwsHeaders.RECEIVED_QUEUE, headers.get("LogicalResourceId"))
					.setHeader(AwsHeaders.ACKNOWLEDGMENT, headers.get("Acknowledgment")).build();

			sendMessage(messageToSend);
		}

	}

}
