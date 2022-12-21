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

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Rahul Pilani
 * @author Artem Bilan
 */
class SqsOutboundChannelAdapterParserTests {

	@Test
	void test_sqs_resource_resolver_defined_with_queue_messaging_template() {
		assertThatExceptionOfType(BeanDefinitionStoreException.class)
				.isThrownBy(() ->
						new ClassPathXmlApplicationContext("SqsOutboundChannelAdapterParserTests-context-bad.xml",
								getClass()));
	}

	@Test
	void test_sqs_defined_with_queue_messaging_template() {
		assertThatExceptionOfType(BeanDefinitionStoreException.class)
				.isThrownBy(() ->
						new ClassPathXmlApplicationContext("SqsOutboundChannelAdapterParserTests-context-bad2.xml",
								getClass()));
	}

	@Test
	void test_resource_resolver_defined_with_queue_messaging_template() {
		assertThatExceptionOfType(BeanDefinitionStoreException.class)
				.isThrownBy(() ->
						new ClassPathXmlApplicationContext("SqsOutboundChannelAdapterParserTests-context-bad3.xml",
								getClass()));
	}

	@Test
	void test_neither_sqs_nor_queue_messaging_template_defined() {
		assertThatExceptionOfType(BeanDefinitionStoreException.class)
				.isThrownBy(() ->
						new ClassPathXmlApplicationContext("SqsOutboundChannelAdapterParserTests-context-bad4.xml",
								getClass()));
	}

}
