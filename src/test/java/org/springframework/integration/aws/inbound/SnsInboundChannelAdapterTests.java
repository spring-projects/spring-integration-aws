/*
 * Copyright 2016-2023 the original author or authors.
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

import java.util.Map;

import io.awspring.cloud.sns.handlers.NotificationStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.ConfirmSubscriptionRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
import org.springframework.test.context.junit.jupiter.web.SpringJUnitWebConfig;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.util.StreamUtils;
import org.springframework.web.HttpRequestHandler;
import org.springframework.web.context.WebApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.verify;
import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * @author Artem Bilan
 * @author Kamil Przerwa
 */
@SpringJUnitWebConfig
@DirtiesContext
public class SnsInboundChannelAdapterTests {

	@Autowired
	private WebApplicationContext context;

	@Autowired
	private SnsClient amazonSns;

	@Autowired
	private PollableChannel inputChannel;

	@Value("classpath:org/springframework/integration/aws/inbound/subscriptionConfirmation.json")
	private Resource subscriptionConfirmation;

	@Value("classpath:org/springframework/integration/aws/inbound/notificationMessage.json")
	private Resource notificationMessage;

	@Value("classpath:org/springframework/integration/aws/inbound/unsubscribeConfirmation.json")
	private Resource unsubscribeConfirmation;

	private MockMvc mockMvc;

	@BeforeEach
	void setUp() {
		this.mockMvc = MockMvcBuilders.webAppContextSetup(this.context).build();
	}

	@Test
	void testSubscriptionConfirmation() throws Exception {
		this.mockMvc
				.perform(post("/mySampleTopic").header("x-amz-sns-message-type", "SubscriptionConfirmation")
						.contentType(MediaType.APPLICATION_JSON)
						.content(StreamUtils.copyToByteArray(this.subscriptionConfirmation.getInputStream())))
				.andExpect(status().isNoContent());

		Message<?> receive = this.inputChannel.receive(10000);
		assertThat(receive).isNotNull();
		assertThat(receive.getHeaders().containsKey(AwsHeaders.SNS_MESSAGE_TYPE)).isTrue();
		assertThat(receive.getHeaders().get(AwsHeaders.SNS_MESSAGE_TYPE)).isEqualTo("SubscriptionConfirmation");

		assertThat(receive.getHeaders().containsKey(AwsHeaders.NOTIFICATION_STATUS)).isTrue();
		NotificationStatus notificationStatus = (NotificationStatus) receive.getHeaders()
				.get(AwsHeaders.NOTIFICATION_STATUS);

		notificationStatus.confirmSubscription();

		verify(this.amazonSns).confirmSubscription(
				ConfirmSubscriptionRequest.builder()
						.topicArn("arn:aws:sns:eu-west-1:111111111111:mySampleTopic")
						.token("111")
						.build());
	}

	@Test
	@SuppressWarnings("unchecked")
	void testNotification() throws Exception {
		this.mockMvc
				.perform(post("/mySampleTopic").header("x-amz-sns-message-type", "Notification")
						.contentType(MediaType.TEXT_PLAIN)
						.content(StreamUtils.copyToByteArray(this.notificationMessage.getInputStream())))
				.andExpect(status().isNoContent());

		Message<?> receive = this.inputChannel.receive(10000);
		assertThat(receive).isNotNull();
		Map<String, String> payload = (Map<String, String>) receive.getPayload();

		assertThat(payload.get("Subject")).isEqualTo("foo");
		assertThat(payload.get("Message")).isEqualTo("bar");
	}

	@Test
	void testUnsubscribe() throws Exception {
		this.mockMvc
				.perform(post("/mySampleTopic").header("x-amz-sns-message-type", "UnsubscribeConfirmation")
						.contentType(MediaType.TEXT_PLAIN)
						.content(StreamUtils.copyToByteArray(this.unsubscribeConfirmation.getInputStream())))
				.andExpect(status().isNoContent());

		Message<?> receive = this.inputChannel.receive(10000);
		assertThat(receive).isNotNull();
		assertThat(receive.getHeaders().containsKey(AwsHeaders.SNS_MESSAGE_TYPE)).isTrue();
		assertThat(receive.getHeaders().get(AwsHeaders.SNS_MESSAGE_TYPE)).isEqualTo("UnsubscribeConfirmation");

		assertThat(receive.getHeaders().containsKey(AwsHeaders.NOTIFICATION_STATUS)).isTrue();
		NotificationStatus notificationStatus = (NotificationStatus) receive.getHeaders()
				.get(AwsHeaders.NOTIFICATION_STATUS);

		notificationStatus.confirmSubscription();

		verify(this.amazonSns).confirmSubscription(
				ConfirmSubscriptionRequest.builder()
						.topicArn("arn:aws:sns:eu-west-1:111111111111:mySampleTopic")
						.token("233")
						.build());
	}

	@Configuration
	@EnableIntegration
	public static class ContextConfiguration {

		@Bean
		public SnsClient amazonSns() {
			return mock(SnsClient.class);
		}

		@Bean
		public PollableChannel inputChannel() {
			return new QueueChannel();
		}

		@Bean
		public HttpRequestHandler snsInboundChannelAdapter() {
			SnsInboundChannelAdapter adapter = new SnsInboundChannelAdapter(amazonSns(), "/mySampleTopic");
			adapter.setRequestChannel(inputChannel());
			adapter.setHandleNotificationStatus(true);
			return adapter;
		}

	}

}
