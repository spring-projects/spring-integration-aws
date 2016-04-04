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

/**
 * The utility class for the namespace parsers
 *
 * @author Amol Nayak
 * @author Artem Bilan
 * @since 0.5
 *
 */
public final class AwsParserUtils {

	public static final String SQS_REF = "sqs";

	public static final String SNS_REF = "sns";

	public static final String S3_REF = "s3";

	public static final String RESOURCE_ID_RESOLVER_REF = "resource-id-resolver";

	private AwsParserUtils() {
		super();
	}

}
