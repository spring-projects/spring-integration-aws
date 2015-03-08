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

import org.w3c.dom.Element;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.integration.aws.outbound.SqsMessageHandler;
import org.springframework.integration.config.xml.AbstractOutboundChannelAdapterParser;
import org.springframework.integration.config.xml.IntegrationNamespaceUtils;
import org.springframework.util.StringUtils;

/**
 * The parser for the {@code <int-aws:sqs-outbound-channel-adapter>}
 *
 * @author Artem Bilan
 */
public class SqsOutboundChannelAdapterParser extends AbstractOutboundChannelAdapterParser {

    public static final String QUEUE_MESSAGING_TEMPLATE_REF = "queue-messaging-template";

    private boolean hasAttribute(Element element, String attribute) {
        return StringUtils.hasText(element.getAttribute(attribute));
    }

	@Override
	protected AbstractBeanDefinition parseConsumer(Element element, ParserContext parserContext) {
		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(SqsMessageHandler.class);

		final boolean hasQueueMessagingTemplate = hasAttribute(element, QUEUE_MESSAGING_TEMPLATE_REF);
		final boolean hasSqs = hasAttribute(element, AmazonWSParserUtils.SQS_REF);
		final boolean hasResourceResolver = hasAttribute(element, AmazonWSParserUtils.RESOURCE_ID_RESOLVER_REF);
		if (hasQueueMessagingTemplate && (hasSqs || hasResourceResolver)) {
			parserContext.getReaderContext().error(QUEUE_MESSAGING_TEMPLATE_REF +
					" should not be defined in conjunction with " + AmazonWSParserUtils.SQS_REF + " or " + AmazonWSParserUtils.RESOURCE_ID_RESOLVER_REF, element);
		}

        if (!hasQueueMessagingTemplate && !hasSqs) {
            parserContext.getReaderContext().error("One of " + QUEUE_MESSAGING_TEMPLATE_REF + " or " + AmazonWSParserUtils.SQS_REF + " must be defined.", element);
        }

        if (hasSqs) {
            builder.addConstructorArgReference(element.getAttribute(AmazonWSParserUtils.SQS_REF));
        }
		if (hasResourceResolver) {
			builder.addConstructorArgReference(element.getAttribute(AmazonWSParserUtils.RESOURCE_ID_RESOLVER_REF));
		}
        if (hasQueueMessagingTemplate) {
            builder.addConstructorArgReference(element.getAttribute(QUEUE_MESSAGING_TEMPLATE_REF));
        }
		BeanDefinition queue = IntegrationNamespaceUtils.createExpressionDefinitionFromValueOrExpression("queue",
				"queue-expression", parserContext, element, false);
		if (queue != null) {
			builder.addPropertyValue("queueExpression", queue);
		}
		return builder.getBeanDefinition();
	}

}
