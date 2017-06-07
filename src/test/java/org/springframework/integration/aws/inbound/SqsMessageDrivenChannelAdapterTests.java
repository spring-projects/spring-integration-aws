/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.integration.aws.inbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.mock;
import static org.mockito.Matchers.any;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.config.ExpressionControlBusFactoryBean;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

/**
 * @author Artem Bilan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@DirtiesContext
public class SqsMessageDrivenChannelAdapterTests {

	@Autowired
	private PollableChannel inputChannel;

	@Autowired
	private SqsMessageDrivenChannelAdapter sqsMessageDrivenChannelAdapter;

	@Autowired
	private MessageChannel controlBusInput;

	@Autowired
	private PollableChannel controlBusOutput;

	@Test
	public void testSqsMessageDrivenChannelAdapter() {
		assertThat(TestUtils.getPropertyValue(this.sqsMessageDrivenChannelAdapter,
				"listenerContainer.queueStopTimeout"))
				.isEqualTo(10000L);
		org.springframework.messaging.Message<?> receive = this.inputChannel.receive(1000);
		assertThat(receive).isNotNull();
		assertThat((String) receive.getPayload()).isIn("messageContent", "messageContent2");
		assertThat(receive.getHeaders().get(AwsHeaders.QUEUE)).isEqualTo("testQueue");
		receive = this.inputChannel.receive(1000);
		assertThat(receive).isNotNull();
		assertThat((String) receive.getPayload()).isIn("messageContent", "messageContent2");
		assertThat(receive.getHeaders().get(AwsHeaders.QUEUE)).isEqualTo("testQueue");

		try {
			this.controlBusInput.send(new GenericMessage<>("@sqsMessageDrivenChannelAdapter.stop('testQueue')"));
		}
		catch (Exception e) {
			// May fail with NPE. See https://github.com/spring-cloud/spring-cloud-aws/issues/232
		}
		this.controlBusInput.send(new GenericMessage<>("@sqsMessageDrivenChannelAdapter.isRunning('testQueue')"));

		receive = this.controlBusOutput.receive(1000);
		assertThat(receive).isNotNull();
		assertThat((Boolean) receive.getPayload()).isFalse();

		this.controlBusInput.send(new GenericMessage<>("@sqsMessageDrivenChannelAdapter.start('testQueue')"));
		this.controlBusInput.send(new GenericMessage<>("@sqsMessageDrivenChannelAdapter.isRunning('testQueue')"));

		receive = this.controlBusOutput.receive(1000);
		assertThat(receive).isNotNull();
		assertThat((Boolean) receive.getPayload()).isTrue();

		assertThatThrownBy(() ->
				this.controlBusInput.send(new GenericMessage<>("@sqsMessageDrivenChannelAdapter.start('foo')")))
				.hasCauseExactlyInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("Queue with name 'foo' does not exist");
	}

	@Configuration
	@EnableIntegration
	public static class ContextConfiguration {

		@Bean
		public AmazonSQSAsync amazonSqs() {
			AmazonSQSAsync sqs = mock(AmazonSQSAsync.class);
			given(sqs.getQueueUrl(new GetQueueUrlRequest("testQueue")))
					.willReturn(new GetQueueUrlResult().withQueueUrl("http://testQueue.amazonaws.com"));


			given(sqs.receiveMessage(new ReceiveMessageRequest("http://testQueue.amazonaws.com")
					.withAttributeNames("All")
					.withMessageAttributeNames("All")
					.withMaxNumberOfMessages(10)))
					.willReturn(new ReceiveMessageResult()
							.withMessages(new Message().withBody("messageContent"),
									new Message().withBody("messageContent2")))
					.willReturn(new ReceiveMessageResult());

			given(sqs.getQueueAttributes(any(GetQueueAttributesRequest.class)))
					.willReturn(new GetQueueAttributesResult());

			return sqs;
		}

		@Bean
		public PollableChannel inputChannel() {
			return new QueueChannel();
		}

		@Bean
		public MessageProducer sqsMessageDrivenChannelAdapter() {
			SqsMessageDrivenChannelAdapter adapter = new SqsMessageDrivenChannelAdapter(amazonSqs(), "testQueue");
			adapter.setOutputChannel(inputChannel());
			return adapter;
		}

		@Bean
		@ServiceActivator(inputChannel = "controlBusInput")
		public ExpressionControlBusFactoryBean controlBus() {
			ExpressionControlBusFactoryBean controlBusFactoryBean = new ExpressionControlBusFactoryBean();
			controlBusFactoryBean.setOutputChannel(controlBusOutput());
			return controlBusFactoryBean;
		}

		@Bean
		public PollableChannel controlBusOutput() {
			return new QueueChannel();
		}

	}

}
