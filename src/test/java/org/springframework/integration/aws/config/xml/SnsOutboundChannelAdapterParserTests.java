/*
 * Copyright 2016 the original author or authors.
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

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.expression.Expression;
import org.springframework.integration.endpoint.AbstractEndpoint;
import org.springframework.integration.handler.advice.RequestHandlerRetryAdvice;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.amazonaws.services.sns.AmazonSNS;

/**
 * @author Artem Bilan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class SnsOutboundChannelAdapterParserTests {

	@Autowired
	private AmazonSNS amazonSns;

	@Autowired
	@Qualifier("defaultAdapter")
	private MessageChannel defaultAdapterChannel;

	@Autowired
	@Qualifier("errorChannel")
	private MessageChannel errorChannel;

	@Autowired
	@Qualifier("defaultAdapter.adapter")
	private AbstractEndpoint defaultAdapter;

	@Autowired
	@Qualifier("defaultAdapter.handler")
	private MessageHandler defaultAdapterHandler;

	@Autowired
	@Qualifier("notificationChannel")
	private MessageChannel notificationChannel;

	@Autowired
	@Qualifier("snsGateway")
	private AbstractEndpoint snsGateway;

	@Autowired
	@Qualifier("snsGateway.handler")
	private MessageHandler snsGatewayHandler;

	@Test
	public void testSnsOutboundChannelAdapterDefaultParser() throws Exception {
		Object handler = TestUtils.getPropertyValue(this.defaultAdapter, "handler");
		assertFalse(AopUtils.isAopProxy(handler));

		assertSame(this.defaultAdapterHandler, handler);

		assertThat(TestUtils.getPropertyValue(handler, "adviceChain", List.class).get(0),
				instanceOf(RequestHandlerRetryAdvice.class));

		assertSame(this.defaultAdapterChannel, TestUtils.getPropertyValue(this.defaultAdapter, "inputChannel"));

		assertSame(this.amazonSns, TestUtils.getPropertyValue(this.defaultAdapterHandler, "amazonSns"));
		assertNotNull(TestUtils.getPropertyValue(this.defaultAdapterHandler, "evaluationContext"));
		assertNull(TestUtils.getPropertyValue(this.defaultAdapterHandler, "topicArnExpression"));
		assertNull(TestUtils.getPropertyValue(this.defaultAdapterHandler, "subjectExpression"));
		assertNull(TestUtils.getPropertyValue(this.defaultAdapterHandler, "bodyExpression"));
	}

	@Test
	public void testSnsOutboundChannelAdapterParser() {
		assertSame(this.notificationChannel, TestUtils.getPropertyValue(this.snsGateway, "inputChannel"));
		assertSame(this.snsGatewayHandler, TestUtils.getPropertyValue(this.snsGateway, "handler"));
		assertFalse(TestUtils.getPropertyValue(this.snsGateway, "autoStartup", Boolean.class));
		assertEquals(new Integer(201), TestUtils.getPropertyValue(this.snsGateway, "phase", Integer.class));
		assertTrue(TestUtils.getPropertyValue(this.snsGatewayHandler, "produceReply", Boolean.class));
		assertSame(this.errorChannel, TestUtils.getPropertyValue(this.snsGatewayHandler, "outputChannel"));

		assertSame(this.amazonSns, TestUtils.getPropertyValue(this.snsGatewayHandler, "amazonSns"));
		assertNotNull(TestUtils.getPropertyValue(this.snsGatewayHandler, "evaluationContext"));
		assertEquals("foo",
				TestUtils.getPropertyValue(this.snsGatewayHandler, "topicArnExpression", Expression.class)
						.getExpressionString());
		assertEquals("bar",
				TestUtils.getPropertyValue(this.snsGatewayHandler, "subjectExpression", Expression.class)
						.getExpressionString());
		assertEquals("payload.toUpperCase()",
				TestUtils.getPropertyValue(this.snsGatewayHandler, "bodyExpression", Expression.class)
						.getExpressionString());
	}

}
