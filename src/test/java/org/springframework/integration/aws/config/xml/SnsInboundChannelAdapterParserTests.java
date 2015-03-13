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

package org.springframework.integration.aws.config.xml;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cloud.aws.messaging.listener.SimpleMessageListenerContainer;
import org.springframework.integration.aws.inbound.SnsInboundChannelAdapter;
import org.springframework.integration.channel.NullChannel;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.amazonaws.services.sns.AmazonSNS;

/**
 * @author Artem Bilan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class SnsInboundChannelAdapterParserTests {

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
	public void testSqsMessageDrivenChannelAdapterParser() {
		assertSame(this.amazonSns, TestUtils.getPropertyValue(this.snsInboundChannelAdapter,
				"notificationStatusResolver.amazonSns"));
		assertTrue(TestUtils.getPropertyValue(this.snsInboundChannelAdapter, "handleNotificationStatus", Boolean.class));
		assertArrayEquals(new String[] {"/foo"},
				TestUtils.getPropertyValue(this.snsInboundChannelAdapter, "requestMapping.pathPatterns", String[].class));
		assertEquals("payload.Message",
				TestUtils.getPropertyValue(this.snsInboundChannelAdapter, "payloadExpression.expression"));
		assertFalse(this.snsInboundChannelAdapter.isRunning());
		assertEquals(100, this.snsInboundChannelAdapter.getPhase());
		assertFalse(this.snsInboundChannelAdapter.isAutoStartup());
		assertSame(this.errorChannel, TestUtils.getPropertyValue(this.snsInboundChannelAdapter, "requestChannel"));
		assertSame(this.nullChannel, TestUtils.getPropertyValue(this.snsInboundChannelAdapter, "errorChannel"));
		assertEquals(2000L, TestUtils.getPropertyValue(this.snsInboundChannelAdapter, "messagingTemplate.sendTimeout"));
	}

}
