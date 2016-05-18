/*
 * Copyright 2015-2016 the original author or authors.
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

import org.junit.Test;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.integration.aws.outbound.SqsMessageHandler;
import org.springframework.integration.test.util.TestUtils;

/**
 * @author Rahul Pilani
 * @author Artem Bilan
 */
public class SqsOutboundChannelAdapterParserTests {

	@Test(expected = BeanDefinitionStoreException.class)
	public void test_sqs_resource_resolver_defined_with_queue_messaging_template() {
		new ClassPathXmlApplicationContext("SqsOutboundChannelAdapterParserTests-context-bad.xml", getClass()).close();
	}

	@Test(expected = BeanDefinitionStoreException.class)
	public void test_sqs_defined_with_queue_messaging_template() {
		new ClassPathXmlApplicationContext("SqsOutboundChannelAdapterParserTests-context-bad2.xml", getClass()).close();
	}

	@Test(expected = BeanDefinitionStoreException.class)
	public void test_resource_resolver_defined_with_queue_messaging_template() {
		new ClassPathXmlApplicationContext("SqsOutboundChannelAdapterParserTests-context-bad3.xml", getClass()).close();
	}

	@Test(expected = BeanDefinitionStoreException.class)
	public void test_neither_sqs_nor_queue_messaging_template_defined() {
		new ClassPathXmlApplicationContext("SqsOutboundChannelAdapterParserTests-context-bad4.xml", getClass()).close();
	}

	@Test
	public void test_happy_path_with_queue_messaging_template() {
		ConfigurableApplicationContext applicationContext =
				new ClassPathXmlApplicationContext("SqsOutboundChannelAdapterParserTests-context-good.xml", getClass());

		SqsMessageHandler handlerWithTemplate =
				applicationContext.getBean("sqsOutboundChannelAdapterWithQueueMessagingTemplate.handler",
						SqsMessageHandler.class);
		assertThat(TestUtils.getPropertyValue(handlerWithTemplate, "template")).isNotNull();

		SqsMessageHandler handlerWithSqs = applicationContext.getBean("sqsOutboundChannelAdapterWithSqs.handler",
				SqsMessageHandler.class);
		assertThat(TestUtils.getPropertyValue(handlerWithSqs, "template")).isNotNull();

		applicationContext.close();
	}

}
