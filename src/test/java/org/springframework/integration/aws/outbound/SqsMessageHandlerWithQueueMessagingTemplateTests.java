/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.integration.aws.outbound;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.cloud.aws.messaging.core.QueueMessagingTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.messaging.MessageHandler;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;

/**
 * Instantiating SqsMessageHandler using QueueMessagingTemplate.
 *
 * @author Rahul Pilani
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class SqsMessageHandlerWithQueueMessagingTemplateTests extends AbstractSqsMessageHandlerTests {


	@Configuration
	@EnableIntegration
	public static class ContextConfiguration {

		@Bean
		public AmazonSQS amazonSqs() {
			AmazonSQS amazonSqs = mock(AmazonSQS.class);

			doAnswer(new Answer<GetQueueUrlResult>() {

				@Override
				public GetQueueUrlResult answer(InvocationOnMock invocation) throws Throwable {
					GetQueueUrlRequest getQueueUrlRequest = (GetQueueUrlRequest) invocation.getArguments()[0];
					GetQueueUrlResult queueUrl = new GetQueueUrlResult();
					queueUrl.setQueueUrl("http://queue-url.com/" + getQueueUrlRequest.getQueueName());
					return queueUrl;
				}

			}).when(amazonSqs).getQueueUrl(any(GetQueueUrlRequest.class));

			return amazonSqs;
		}

		@Bean
		public QueueMessagingTemplate queueMessagingTemplate() {
			return new QueueMessagingTemplate(amazonSqs());
		}

		@Bean
		@ServiceActivator(inputChannel = "sqsSendChannel")
		public MessageHandler sqsMessageHandler() {
			return new SqsMessageHandler(queueMessagingTemplate());
		}

	}

}
