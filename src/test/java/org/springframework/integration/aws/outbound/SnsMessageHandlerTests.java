/*
 * Copyright 2016-2019 the original author or authors.
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

package org.springframework.integration.aws.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.aws.support.AwsHeaders;
import org.springframework.integration.aws.support.SnsBodyBuilder;
import org.springframework.integration.aws.support.SnsHeaderMapper;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.PollableChannel;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

/**
 * @author Artem Bilan
 */
@SpringJUnitConfig
@DirtiesContext
public class SnsMessageHandlerTests {

	private static SpelExpressionParser PARSER = new SpelExpressionParser();

	@Autowired
	private MessageChannel sendToSnsChannel;

	@Autowired
	private AmazonSNSAsync amazonSNS;

	@Autowired
	private PollableChannel resultChannel;

	@Test
	@SuppressWarnings("unchecked")
	void testSnsMessageHandler() {
		SnsBodyBuilder payload = SnsBodyBuilder.withDefault("foo").forProtocols("{\"foo\" : \"bar\"}", "sms");

		Message<?> message = MessageBuilder.withPayload(payload).setHeader("topic", "topic")
				.setHeader("subject", "subject").setHeader("foo", "bar").build();

		this.sendToSnsChannel.send(message);

		Message<?> reply = this.resultChannel.receive(10000);
		assertThat(reply).isNotNull();

		ArgumentCaptor<PublishRequest> captor = ArgumentCaptor.forClass(PublishRequest.class);
		verify(this.amazonSNS).publishAsync(captor.capture(), any(AsyncHandler.class));

		PublishRequest publishRequest = captor.getValue();

		assertThat(publishRequest.getMessageStructure()).isEqualTo("json");
		assertThat(publishRequest.getTopicArn()).isEqualTo("topic");
		assertThat(publishRequest.getSubject()).isEqualTo("subject");
		assertThat(publishRequest.getMessage())
				.isEqualTo("{\"default\":\"foo\",\"sms\":\"{\\\"foo\\\" : \\\"bar\\\"}\"}");

		Map<String, MessageAttributeValue> messageAttributes = publishRequest.getMessageAttributes();

		assertThat(messageAttributes).doesNotContainKey(MessageHeaders.ID);
		assertThat(messageAttributes).doesNotContainKey(MessageHeaders.TIMESTAMP);
		assertThat(messageAttributes).containsKey("foo");
		assertThat(messageAttributes.get("foo").getStringValue()).isEqualTo("bar");

		assertThat(reply.getHeaders().get(AwsHeaders.MESSAGE_ID)).isEqualTo("111");
		assertThat(reply.getHeaders().get(AwsHeaders.TOPIC)).isEqualTo("topic");
		assertThat(reply.getPayload()).isSameAs(payload);
	}

	@Configuration
	@EnableIntegration
	public static class ContextConfiguration {

		@Bean
		@SuppressWarnings("unchecked")
		public AmazonSNSAsync amazonSNS() {
			AmazonSNSAsync mock = mock(AmazonSNSAsync.class);

			willAnswer(invocation -> {
				PublishResult publishResult = new PublishResult().withMessageId("111");
				AsyncHandler<PublishRequest, PublishResult> asyncHandler = invocation.getArgument(1);
				asyncHandler.onSuccess(invocation.getArgument(0), publishResult);
				return new AsyncResult<>(publishResult);
			}).given(mock).publishAsync(any(PublishRequest.class), any(AsyncHandler.class));

			return mock;
		}

		@Bean
		public PollableChannel resultChannel() {
			return new QueueChannel();
		}

		@Bean
		@ServiceActivator(inputChannel = "sendToSnsChannel")
		public MessageHandler snsMessageHandler() {
			SnsMessageHandler snsMessageHandler = new SnsMessageHandler(amazonSNS());
			snsMessageHandler.setTopicArnExpression(PARSER.parseExpression("headers.topic"));
			snsMessageHandler.setSubjectExpression(PARSER.parseExpression("headers.subject"));
			snsMessageHandler.setBodyExpression(PARSER.parseExpression("payload"));
			snsMessageHandler.setOutputChannel(resultChannel());
			SnsHeaderMapper headerMapper = new SnsHeaderMapper();
			headerMapper.setOutboundHeaderNames("foo");
			snsMessageHandler.setHeaderMapper(headerMapper);
			return snsMessageHandler;
		}

	}

}
