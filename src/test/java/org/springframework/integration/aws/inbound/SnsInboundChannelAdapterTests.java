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

package org.springframework.integration.aws.inbound;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.aws.messaging.endpoint.NotificationStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.messaging.Message;
import org.springframework.messaging.PollableChannel;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.util.StreamUtils;
import org.springframework.web.HttpRequestHandler;
import org.springframework.web.context.WebApplicationContext;

import com.amazonaws.services.sns.AmazonSNS;

/**
 * @author Artem Bilan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@WebAppConfiguration
@DirtiesContext
public class SnsInboundChannelAdapterTests {

	@Autowired
	private WebApplicationContext context;

	@Autowired
	private AmazonSNS amazonSns;

	@Autowired
	private PollableChannel inputChannel;

	@Value("classpath:org/springframework/integration/aws/inbound/subscriptionConfirmation.json")
	private Resource subscriptionConfirmation;

	@Value("classpath:org/springframework/integration/aws/inbound/notificationMessage.json")
	private Resource notificationMessage;

	@Value("classpath:org/springframework/integration/aws/inbound/unsubscribeConfirmation.json")
	private Resource unsubscribeConfirmation;

	private MockMvc mockMvc;

	@Before
	public void setUp() throws Exception {
		this.mockMvc = MockMvcBuilders.webAppContextSetup(this.context).build();
	}

	@Test
	public void testSubscriptionConfirmation() throws Exception {
		this.mockMvc.perform(
				post("/mySampleTopic")
						.header("x-amz-sns-message-type", "SubscriptionConfirmation")
						.contentType(MediaType.APPLICATION_JSON)
						.content(StreamUtils.copyToByteArray(this.subscriptionConfirmation.getInputStream())))
				.andExpect(status().isNoContent());

		Message<?> receive = inputChannel.receive(10000);
		assertNotNull(receive);
		assertTrue(receive.getHeaders().containsKey(AwsHeaders.SNS_MESSAGE_TYPE));
		assertEquals("SubscriptionConfirmation", receive.getHeaders().get(AwsHeaders.SNS_MESSAGE_TYPE));

		assertTrue(receive.getHeaders().containsKey(AwsHeaders.NOTIFICATION_STATUS));
		NotificationStatus notificationStatus = (NotificationStatus) receive.getHeaders()
				.get(AwsHeaders.NOTIFICATION_STATUS);

		notificationStatus.confirmSubscription();

		verify(this.amazonSns)
				.confirmSubscription("arn:aws:sns:eu-west-1:111111111111:mySampleTopic", "111");
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testNotification() throws Exception {
		this.mockMvc.perform(
				post("/mySampleTopic")
						.header("x-amz-sns-message-type", "Notification")
						.contentType(MediaType.APPLICATION_JSON)
						.content(StreamUtils.copyToByteArray(this.notificationMessage.getInputStream())))
				.andExpect(status().isNoContent());

		Message<?> receive = inputChannel.receive(10000);
		assertNotNull(receive);
		Map<String, String> payload = (Map<String, String>) receive.getPayload();

		assertEquals("foo", payload.get("Subject"));
		assertEquals("bar", payload.get("Message"));
	}

	@Test
	public void testUnsubscribe() throws Exception {
		this.mockMvc.perform(
				post("/mySampleTopic")
						.header("x-amz-sns-message-type", "UnsubscribeConfirmation")
						.contentType(MediaType.APPLICATION_JSON)
						.content(StreamUtils.copyToByteArray(this.unsubscribeConfirmation.getInputStream())))
				.andExpect(status().isNoContent());

		Message<?> receive = inputChannel.receive(10000);
		assertNotNull(receive);
		assertTrue(receive.getHeaders().containsKey(AwsHeaders.SNS_MESSAGE_TYPE));
		assertEquals("UnsubscribeConfirmation", receive.getHeaders().get(AwsHeaders.SNS_MESSAGE_TYPE));

		assertTrue(receive.getHeaders().containsKey(AwsHeaders.NOTIFICATION_STATUS));
		NotificationStatus notificationStatus = (NotificationStatus) receive.getHeaders()
				.get(AwsHeaders.NOTIFICATION_STATUS);

		notificationStatus.confirmSubscription();

		verify(this.amazonSns)
				.confirmSubscription("arn:aws:sns:eu-west-1:111111111111:mySampleTopic", "233");
	}

	@Configuration
	@EnableIntegration
	public static class ContextConfiguration {

		@Bean
		public AmazonSNS amazonSns() {
			return Mockito.mock(AmazonSNS.class);
		}

		@Bean
		public PollableChannel inputChannel() {
			return new QueueChannel();
		}

		@Bean
		public HttpRequestHandler sqsMessageDrivenChannelAdapter() {
			SnsInboundChannelAdapter adapter = new SnsInboundChannelAdapter(amazonSns(), "/mySampleTopic");
			adapter.setRequestChannel(inputChannel());
			adapter.setHandleNotificationStatus(true);
			return adapter;
		}

	}

}
