/*
 * Copyright 2016-2019 the original author or authors.
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

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.integration.aws.outbound.S3MessageHandler;
import org.springframework.integration.config.xml.AbstractConsumerEndpointParser;
import org.springframework.integration.config.xml.IntegrationNamespaceUtils;
import org.springframework.util.StringUtils;

/**
 * The parser for the {@code <int-aws:s3-outbound-gateway>}.
 *
 * @author Artem Bilan
 */
public class S3OutboundGatewayParser extends AbstractConsumerEndpointParser {

	@Override
	protected String getInputChannelAttributeName() {
		return "request-channel";
	}

	@Override
	protected BeanDefinitionBuilder parseHandler(Element element, ParserContext parserContext) {
		String s3 = element.getAttribute(AwsParserUtils.S3_REF);
		boolean hasS3 = StringUtils.hasText(s3);
		String transferManager = element.getAttribute("transfer-manager");
		boolean hasTransferManager = StringUtils.hasText(transferManager);

		if (hasS3 == hasTransferManager) {
			parserContext.getReaderContext()
					.error("One and only of 's3' and 'transfer-manager' attributes must be provided", element);
		}

		BeanDefinition bucketExpression = IntegrationNamespaceUtils.createExpressionDefinitionFromValueOrExpression(
				"bucket", "bucket-expression", parserContext, element, true);

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(S3MessageHandler.class)
				.addConstructorArgReference(hasS3 ? s3 : transferManager).addConstructorArgValue(bucketExpression)
				.addConstructorArgValue(true);

		BeanDefinition commandExpression = IntegrationNamespaceUtils.createExpressionDefinitionFromValueOrExpression(
				"command", "command-expression", parserContext, element, false);

		if (commandExpression != null) {
			builder.addPropertyValue("commandExpression", commandExpression);
		}

		IntegrationNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "progress-listener");
		IntegrationNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "upload-metadata-provider");

		BeanDefinition keyExpression = IntegrationNamespaceUtils.createExpressionDefIfAttributeDefined("key-expression",
				element);
		if (keyExpression != null) {
			builder.addPropertyValue("keyExpression", keyExpression);
		}

		BeanDefinition objectAclExpression = IntegrationNamespaceUtils
				.createExpressionDefIfAttributeDefined("object-acl-expression", element);
		if (objectAclExpression != null) {
			builder.addPropertyValue("objectAclExpression", objectAclExpression);
		}

		BeanDefinition destinationBucketExpression = IntegrationNamespaceUtils
				.createExpressionDefIfAttributeDefined("destination-bucket-expression", element);
		if (destinationBucketExpression != null) {
			builder.addPropertyValue("destinationBucketExpression", destinationBucketExpression);
		}

		BeanDefinition destinationKeyExpression = IntegrationNamespaceUtils
				.createExpressionDefIfAttributeDefined("destination-key-expression", element);
		if (destinationKeyExpression != null) {
			builder.addPropertyValue("destinationKeyExpression", destinationKeyExpression);
		}

		IntegrationNamespaceUtils.setValueIfAttributeDefined(builder, element, "reply-timeout", "sendTimeout");
		IntegrationNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "reply-channel", "outputChannel");
		IntegrationNamespaceUtils.setReferenceIfAttributeDefined(builder, element, "resource-id-resolver");

		return builder;
	}

}
