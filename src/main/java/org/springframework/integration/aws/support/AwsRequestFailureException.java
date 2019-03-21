/*
 * Copyright 2017 the original author or authors.
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

import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;

import com.amazonaws.AmazonWebServiceRequest;

/**
 * An exception that is the payload of an {@code ErrorMessage} when a send fails.
 *
 * @author Jacob Severson
 *
 * @since 1.1
 */
public class AwsRequestFailureException extends MessagingException {

	private static final long serialVersionUID = 1L;

	private final AmazonWebServiceRequest request;

	public AwsRequestFailureException(Message<?> message, AmazonWebServiceRequest request, Throwable cause) {
		super(message, cause);
		this.request = request;
	}

	public AmazonWebServiceRequest getRequest() {
		return this.request;
	}

	@Override
	public String toString() {
		return super.toString() + " [request=" + this.request + "]";
	}

}
