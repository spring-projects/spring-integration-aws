/*
 * Copyright 2018-2021 the original author or authors.
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

package org.springframework.integration.aws.support;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.integration.mapping.HeaderMapper;
import org.springframework.integration.support.utils.PatternMatchUtils;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.util.NumberUtils;

import io.awspring.cloud.messaging.core.MessageAttributeDataTypes;

/**
 * Base {@link HeaderMapper} implementation for common logic in SQS and SNS around message
 * attributes mapping.
 *
 * The {@link #toHeaders(Map)} is not supported.
 *
 * @param <A> the target message attribute type.
 * @author Artem Bilan
 * @since 2.0
 */
public abstract class AbstractMessageAttributesHeaderMapper<A> implements HeaderMapper<Map<String, A>> {

	private static final Log logger = LogFactory.getLog(SqsHeaderMapper.class);

	private volatile String[] outboundHeaderNames = {"!" + MessageHeaders.ID, "!" + MessageHeaders.TIMESTAMP,
			"!" + AwsHeaders.MESSAGE_ID, "!" + AwsHeaders.QUEUE, "!" + AwsHeaders.TOPIC, "*"};

	/**
	 * Provide the header names that should be mapped to a AWS request object attributes
	 * (for outbound adapters) from a Spring Integration Message's headers. The values can
	 * also contain simple wildcard patterns (e.g. "foo*" or "*foo") to be matched. Also
	 * supports negated ('!') patterns. First match wins (positive or negative). To match
	 * the names starting with {@code !} symbol, you have to escape it prepending with the
	 * {@code \} symbol in the pattern definition. Defaults to map all ({@code *}) if the
	 * type is supported by SQS. The {@link MessageHeaders#ID},
	 * {@link MessageHeaders#TIMESTAMP}, {@link AwsHeaders#MESSAGE_ID},
	 * {@link AwsHeaders#QUEUE} and {@link AwsHeaders#TOPIC} are ignored by default.
	 * @param outboundHeaderNames The inbound header names.
	 */
	public void setOutboundHeaderNames(String... outboundHeaderNames) {
		Assert.notNull(outboundHeaderNames, "'outboundHeaderNames' must not be null.");
		Assert.noNullElements(outboundHeaderNames, "'outboundHeaderNames' must not contains null elements.");
		Arrays.sort(outboundHeaderNames);
		this.outboundHeaderNames = outboundHeaderNames;
	}

	@Override
	public void fromHeaders(MessageHeaders headers, Map<String, A> target) {
		for (Map.Entry<String, Object> messageHeader : headers.entrySet()) {
			String messageHeaderName = messageHeader.getKey();
			Object messageHeaderValue = messageHeader.getValue();

			if (Boolean.TRUE.equals(PatternMatchUtils.smartMatch(messageHeaderName, this.outboundHeaderNames))) {

				if (messageHeaderValue instanceof UUID || messageHeaderValue instanceof MimeType
						|| messageHeaderValue instanceof Boolean || messageHeaderValue instanceof String) {

					target.put(messageHeaderName, getStringMessageAttribute(messageHeaderValue.toString()));
				}
				else if (messageHeaderValue instanceof Number) {
					target.put(messageHeaderName, getNumberMessageAttribute(messageHeaderValue));
				}
				else if (messageHeaderValue instanceof ByteBuffer) {
					target.put(messageHeaderName, getBinaryMessageAttribute((ByteBuffer) messageHeaderValue));
				}
				else if (messageHeaderValue instanceof byte[]) {
					target.put(messageHeaderName,
							getBinaryMessageAttribute(ByteBuffer.wrap((byte[]) messageHeaderValue)));
				}
				else {
					if (logger.isWarnEnabled()) {
						logger.warn(String.format(
								"Message header with name '%s' and type '%s' cannot be sent as"
										+ " message attribute because it is not supported by SQS.",
								messageHeaderName, messageHeaderValue.getClass().getName()));
					}
				}

			}
		}
	}

	private A getBinaryMessageAttribute(ByteBuffer messageHeaderValue) {
		return buildMessageAttribute(MessageAttributeDataTypes.BINARY, messageHeaderValue);
	}

	private A getStringMessageAttribute(String messageHeaderValue) {
		return buildMessageAttribute(MessageAttributeDataTypes.STRING, messageHeaderValue);
	}

	private A getNumberMessageAttribute(Object messageHeaderValue) {
		Assert.isTrue(NumberUtils.STANDARD_NUMBER_TYPES.contains(messageHeaderValue.getClass()),
				"Only standard number types are accepted as message header.");

		return buildMessageAttribute(MessageAttributeDataTypes.NUMBER + "." + messageHeaderValue.getClass().getName(),
				messageHeaderValue);
	}

	protected abstract A buildMessageAttribute(String dataType, Object value);

	@Override
	public Map<String, Object> toHeaders(Map<String, A> source) {
		throw new UnsupportedOperationException("The mapping from AWS Response Message is not supported");
	}

}
