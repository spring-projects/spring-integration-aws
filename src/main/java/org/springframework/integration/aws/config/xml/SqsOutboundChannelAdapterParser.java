/*
 * Copyright 2015-2020 the original author or authors.
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

import org.w3c.dom.Element;

import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.integration.aws.outbound.SqsMessageHandler;
import org.springframework.integration.config.xml.AbstractOutboundChannelAdapterParser;
import org.springframework.integration.config.xml.IntegrationNamespaceUtils;
import org.springframework.util.StringUtils;

/**
 * The parser for the {@code <int-aws:sqs-outbound-channel-adapter>}.
 *
 * @author Artem Bilan
 * @author Rahul Pilani
 */
public class SqsOutboundChannelAdapterParser extends AbstractOutboundChannelAdapterParser {

	@Override
	protected AbstractBeanDefinition parseConsumer(Element element, ParserContext parserContext) {
		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(SqsMessageHandler.class);

		String resourceIdResolver = element.getAttribute(AwsParserUtils.RESOURCE_ID_RESOLVER_REF);
		boolean hasResourceIdResolver = StringUtils.hasText(resourceIdResolver);

		builder.addConstructorArgReference(element.getAttribute(AwsParserUtils.SQS_REF));

		if (hasResourceIdResolver) {
			builder.addConstructorArgReference(resourceIdResolver);
		}

		IntegrationNamespaceUtils.setValueIfAttributeDefined(builder, element, "sync");
		IntegrationNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "message-converter");
		IntegrationNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "error-message-strategy");
		IntegrationNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "failure-channel");
		IntegrationNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "async-handler");
		IntegrationNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "success-channel", "outputChannel");

		AwsParserUtils.populateExpressionAttribute("queue", builder, element, parserContext);
		AwsParserUtils.populateExpressionAttribute("delay", builder, element, parserContext);
		AwsParserUtils.populateExpressionAttribute("message-group-id", builder, element, parserContext);
		AwsParserUtils.populateExpressionAttribute("message-deduplication-id", builder, element, parserContext);
		AwsParserUtils.populateExpressionAttribute("send-timeout", builder, element, parserContext);

		return builder.getBeanDefinition();
	}

}
