/*
 * Copyright 2015-2022 the original author or authors.
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

import com.amazonaws.services.sns.AmazonSNS;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.integration.aws.inbound.SnsInboundChannelAdapter;
import org.springframework.integration.channel.NullChannel;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Artem Bilan
 */
@SpringJUnitConfig
@DirtiesContext
class SnsInboundChannelAdapterParserTests {

	@Autowired
	private AmazonSNS amazonSns;

	@Autowired
	private MessageChannel errorChannel;

	@Autowired
	private NullChannel nullChannel;

	@Autowired
	@Qualifier("snsInboundChannelAdapter")
	private SnsInboundChannelAdapter snsInboundChannelAdapter;

	@Test
	void testSnsInboundChannelAdapterParser() {
		assertThat(TestUtils.getPropertyValue(this.snsInboundChannelAdapter, "notificationStatusResolver.amazonSns"))
				.isSameAs(this.amazonSns);
		assertThat(TestUtils.getPropertyValue(this.snsInboundChannelAdapter, "handleNotificationStatus", Boolean.class))
				.isTrue();
		assertThat(TestUtils.getPropertyValue(this.snsInboundChannelAdapter, "requestMapping.pathPatterns",
				String[].class)).isEqualTo(new String[] { "/foo" });
		assertThat(TestUtils.getPropertyValue(this.snsInboundChannelAdapter, "payloadExpression.expression"))
				.isEqualTo("payload.Message");
		assertThat(this.snsInboundChannelAdapter.isRunning()).isFalse();
		assertThat(this.snsInboundChannelAdapter.getPhase()).isEqualTo(100);
		assertThat(this.snsInboundChannelAdapter.isAutoStartup()).isFalse();
		assertThat(TestUtils.getPropertyValue(this.snsInboundChannelAdapter, "requestChannel"))
				.isSameAs(this.errorChannel);
		assertThat(TestUtils.getPropertyValue(this.snsInboundChannelAdapter, "errorChannel"))
				.isSameAs(this.nullChannel);
		assertThat(TestUtils.getPropertyValue(this.snsInboundChannelAdapter, "messagingTemplate.sendTimeout"))
				.isEqualTo(2000L);
	}

}
