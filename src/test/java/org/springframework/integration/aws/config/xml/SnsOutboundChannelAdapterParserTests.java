/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.integration.aws.config.xml;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cloud.aws.core.env.ResourceIdResolver;
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

	@Autowired
	private ResourceIdResolver resourceIdResolver;

	@Test
	public void testSnsOutboundChannelAdapterDefaultParser() throws Exception {
		Object handler = TestUtils.getPropertyValue(this.defaultAdapter, "handler");
		assertThat(AopUtils.isAopProxy(handler)).isFalse();

		assertThat(handler).isSameAs(this.defaultAdapterHandler);

		assertThat(TestUtils.getPropertyValue(handler, "adviceChain", List.class).get(0))
				.isInstanceOf(RequestHandlerRetryAdvice.class);

		assertThat(TestUtils.getPropertyValue(this.defaultAdapter, "inputChannel"))
				.isSameAs(this.defaultAdapterChannel);

		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "amazonSns")).isSameAs(this.amazonSns);
		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "evaluationContext")).isNotNull();
		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "topicArnExpression")).isNull();
		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "subjectExpression")).isNull();
		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "bodyExpression")).isNull();
		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "resourceIdResolver"))
				.isSameAs(this.resourceIdResolver);
	}

	@Test
	public void testSnsOutboundGatewayParser() {
		assertThat(TestUtils.getPropertyValue(this.snsGateway, "inputChannel")).isSameAs(this.notificationChannel);
		assertThat(TestUtils.getPropertyValue(this.snsGateway, "handler")).isSameAs(this.snsGatewayHandler);
		assertThat(TestUtils.getPropertyValue(this.snsGateway, "autoStartup", Boolean.class)).isFalse();
		assertThat(TestUtils.getPropertyValue(this.snsGateway, "phase", Integer.class)).isEqualTo(201);
		assertThat(TestUtils.getPropertyValue(this.snsGatewayHandler, "produceReply", Boolean.class)).isTrue();
		assertThat(TestUtils.getPropertyValue(this.snsGatewayHandler, "outputChannel")).isSameAs(this.errorChannel);
		assertThat(TestUtils.getPropertyValue(this.snsGatewayHandler, "resourceIdResolver"))
				.isSameAs(this.resourceIdResolver);

		assertThat(TestUtils.getPropertyValue(this.snsGatewayHandler, "amazonSns")).isSameAs(this.amazonSns);
		assertThat(TestUtils.getPropertyValue(this.snsGatewayHandler, "evaluationContext")).isNotNull();
		assertThat(TestUtils.getPropertyValue(this.snsGatewayHandler, "topicArnExpression", Expression.class)
				.getExpressionString())
				.isEqualTo("foo");
		assertThat(TestUtils.getPropertyValue(this.snsGatewayHandler, "subjectExpression", Expression.class)
				.getExpressionString())
				.isEqualTo("bar");
		assertThat(TestUtils.getPropertyValue(this.snsGatewayHandler, "bodyExpression", Expression.class)
				.getExpressionString())
				.isEqualTo("payload.toUpperCase()");
	}

}
