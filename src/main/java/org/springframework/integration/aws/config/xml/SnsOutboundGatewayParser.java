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

import org.w3c.dom.Element;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.integration.aws.outbound.SnsMessageHandler;
import org.springframework.integration.config.xml.AbstractConsumerEndpointParser;
import org.springframework.integration.config.xml.IntegrationNamespaceUtils;

/**
 * @author Artem Bilan
 */
public class SnsOutboundGatewayParser extends AbstractConsumerEndpointParser {

	@Override
	protected String getInputChannelAttributeName() {
		return "request-channel";
	}

	@Override
	protected BeanDefinitionBuilder parseHandler(Element element, ParserContext parserContext) {
		String sns = element.getAttribute(AwsParserUtils.SNS_REF);

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(SnsMessageHandler.class)
				.addConstructorArgReference(sns)
				.addConstructorArgValue(true);

		BeanDefinition topic = IntegrationNamespaceUtils.createExpressionDefinitionFromValueOrExpression("topic-arn",
				"topic-arn-expression", parserContext, element, false);
		if (topic != null) {
			builder.addPropertyValue("topicArnExpression", topic);
		}

		BeanDefinition subject = IntegrationNamespaceUtils.createExpressionDefinitionFromValueOrExpression("subject",
				"subject-expression", parserContext, element, false);
		if (subject != null) {
			builder.addPropertyValue("subjectExpression", subject);
		}

		BeanDefinition message =
				IntegrationNamespaceUtils.createExpressionDefIfAttributeDefined("body-expression", element);
		if (message != null) {
			builder.addPropertyValue("bodyExpression", message);
		}

		IntegrationNamespaceUtils.setValueIfAttributeDefined(builder, element, "reply-timeout", "sendTimeout");
		IntegrationNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "reply-channel", "outputChannel");

		return builder;
	}

}
