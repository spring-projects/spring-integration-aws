/*
 * Copyright 2002-2016 the original author or authors.
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

import org.w3c.dom.Element;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.core.Conventions;
import org.springframework.integration.config.xml.IntegrationNamespaceUtils;

/**
 * The utility class for the namespace parsers.
 *
 * @author Amol Nayak
 * @author Artem Bilan
 * @since 0.5
 *
 */
public final class AwsParserUtils {

	/**
	 * The 'sqs' reference attribute name.
	 */
	public static final String SQS_REF = "sqs";

	/**
	 * The 'sns' reference attribute name.
	 */
	public static final String SNS_REF = "sns";

	/**
	 * The 's3' reference attribute name.
	 */
	public static final String S3_REF = "s3";

	/**
	 * The 'resource-id-resolver' reference attribute name.
	 */
	public static final String RESOURCE_ID_RESOLVER_REF = "resource-id-resolver";

	private AwsParserUtils() {
		super();
	}

	static void populateExpressionAttribute(String attributeName, BeanDefinitionBuilder builder,
			Element element, ParserContext parserContext) {

		BeanDefinition beanDefinition =
				IntegrationNamespaceUtils.createExpressionDefinitionFromValueOrExpression(attributeName,
						attributeName + "-expression", parserContext, element, false);
		if (beanDefinition != null) {
			builder.addPropertyValue(Conventions.attributeNameToPropertyName(attributeName) + "Expression",
					beanDefinition);
		}
	}

}
