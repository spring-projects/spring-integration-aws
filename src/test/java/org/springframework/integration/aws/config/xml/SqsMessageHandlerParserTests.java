/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.integration.aws.config.xml;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cloud.aws.core.env.ResourceIdResolver;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.amazonaws.services.sqs.AmazonSQS;

/**
 * @author Artem Bilan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@DirtiesContext
public class SqsMessageHandlerParserTests {

	@Autowired
	private AmazonSQS amazonSqs;

	@Autowired
	private ResourceIdResolver resourceIdResolver;

	@Autowired
	private MessageChannel errorChannel;

	@Autowired
	private EventDrivenConsumer sqsOutboundChannelAdapter;

	@Autowired
	@Qualifier("sqsOutboundChannelAdapter.handler")
	private MessageHandler sqsOutboundChannelAdapterHandler;

	@Test
	public void testSqsMessageHandlerParser() {
		assertThat(TestUtils.getPropertyValue(this.sqsOutboundChannelAdapterHandler, "template.amazonSqs"))
				.isSameAs(this.amazonSqs);
		assertThat(TestUtils.getPropertyValue(this.sqsOutboundChannelAdapterHandler,
				"template.destinationResolver.targetDestinationResolver.resourceIdResolver"))
				.isSameAs(this.resourceIdResolver);
		assertThat(TestUtils.getPropertyValue(this.sqsOutboundChannelAdapterHandler,
				"queueExpression.literalValue"))
				.isEqualTo("foo");
		assertThat(this.sqsOutboundChannelAdapter.getPhase()).isEqualTo(100);
		assertThat(this.sqsOutboundChannelAdapter.isAutoStartup()).isFalse();
		assertThat(this.sqsOutboundChannelAdapter.isRunning()).isFalse();
		assertThat(TestUtils.getPropertyValue(this.sqsOutboundChannelAdapter, "inputChannel"))
				.isSameAs(this.errorChannel);
		assertThat(TestUtils.getPropertyValue(this.sqsOutboundChannelAdapter, "handler"))
				.isSameAs(this.sqsOutboundChannelAdapterHandler);
	}

}
