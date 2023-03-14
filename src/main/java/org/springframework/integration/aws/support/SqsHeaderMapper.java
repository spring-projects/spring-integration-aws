/*
 * Copyright 2018-2023 the original author or authors.
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

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import org.springframework.messaging.MessageHeaders;

/**
 * The {@link AbstractMessageAttributesHeaderMapper} implementation for the mapping from
 * headers to SQS message attributes.
 * <p>
 * The
 * {@link io.awspring.cloud.sqs.listener.SqsMessageListenerContainer}
 * maps all the SQS message attributes to the {@link MessageHeaders}.
 *
 * @author Artem Bilan
 *
 * @since 2.0
 */
public class SqsHeaderMapper extends AbstractMessageAttributesHeaderMapper<MessageAttributeValue> {

	@Override
	protected MessageAttributeValue buildMessageAttribute(String dataType, Object value) {
		MessageAttributeValue.Builder messageAttributeValue =
				MessageAttributeValue.builder()
						.dataType(dataType);
		if (value instanceof ByteBuffer byteBuffer) {
			messageAttributeValue.binaryValue(SdkBytes.fromByteBuffer(byteBuffer));
		}
		else {
			messageAttributeValue.stringValue(value.toString());
		}

		return messageAttributeValue.build();
	}

}
