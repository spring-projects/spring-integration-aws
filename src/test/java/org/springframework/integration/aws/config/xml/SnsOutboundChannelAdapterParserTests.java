/*
 * Copyright 2016-2022 the original author or authors.
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

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sns.AmazonSNSAsync;
import io.awspring.cloud.core.env.ResourceIdResolver;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.integration.endpoint.AbstractEndpoint;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Artem Bilan
 * @author Christopher Smith
 */
@SpringJUnitConfig
@DirtiesContext
class SnsOutboundChannelAdapterParserTests {

	@Autowired
	private AmazonSNSAsync amazonSns;

	@Autowired
	@Qualifier("errorChannel")
	private MessageChannel errorChannel;

	@Autowired
	@Qualifier("defaultAdapter")
	private AbstractEndpoint defaultAdapter;

	@Autowired
	@Qualifier("defaultAdapter.handler")
	private MessageHandler defaultAdapterHandler;

	@Autowired
	@Qualifier("notificationChannel")
	private MessageChannel notificationChannel;

	@Autowired
	private ResourceIdResolver resourceIdResolver;

	@Autowired
	private ErrorMessageStrategy errorMessageStrategy;

	@Autowired
	private AsyncHandler<?, ?> asyncHandler;

	@Autowired
	private MessageChannel successChannel;

	@Test
	void testSnsOutboundChannelAdapterDefaultParser() {
		assertThat(TestUtils.getPropertyValue(this.defaultAdapter, "inputChannel")).isSameAs(this.notificationChannel);

		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "amazonSns")).isSameAs(this.amazonSns);
		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "evaluationContext")).isNotNull();
		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "topicArnExpression")).isNull();
		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "messageGroupIdExpression")).isNull();
		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "messageDeduplicationIdExpression")).isNull();
		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "subjectExpression")).isNull();
		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "bodyExpression")).isNull();
		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "resourceIdResolver"))
				.isSameAs(this.resourceIdResolver);
		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "failureChannel"))
				.isSameAs(this.errorChannel);

		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "resourceIdResolver"))
				.isSameAs(this.resourceIdResolver);

		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "outputChannel"))
				.isSameAs(this.successChannel);

		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "errorMessageStrategy"))
				.isSameAs(this.errorMessageStrategy);

		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "asyncHandler")).isSameAs(this.asyncHandler);

		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "sync", Boolean.class)).isFalse();

		assertThat(TestUtils.getPropertyValue(this.defaultAdapterHandler, "sendTimeoutExpression.literalValue"))
				.isEqualTo("202");
	}

}
