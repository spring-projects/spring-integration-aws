/*
 * Copyright 2002-2014 the original author or authors.
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
package org.springframework.integration.aws.s3.config.xml;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.integration.aws.s3.core.AmazonS3Operations;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.handler.advice.AbstractRequestHandlerAdvice;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.GenericMessage;

import static org.junit.Assert.assertEquals;
import static org.springframework.integration.test.util.TestUtils.getPropertyValue;

/**
 * The test case for the aws-s3 namespace's {@link AmazonS3OutboundChannelAdapterParser} class by
 * connecting to AWS.
 *
 * @author Karthikeyan Palanivelu
 *
 * @since 1.0
 *
 */
@Ignore
public class AmazonS3OutboundChannelAdapterParserAWSTests {


	/**
	 * Test case for the xml definition with a custom implementation of {@link AmazonS3Operations}
	 *
	 */
	private volatile static int adviceCalled;

	/**
	 * Tests the outbound channel adapter definition with a valid combination of attributes along with
	 * request handler chain.
	 *
	 * Need AWS Connectivity.
	 *
	 */
	@Test
	public void withHandlerChain(){
		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("s3-valid-outbound-advice-cases.xml");
		EventDrivenConsumer consumer = ctx.getBean("withHandlerChain",EventDrivenConsumer.class);
		MessageHandler handler = getPropertyValue(consumer, "handler", MessageHandler.class);	
		handler.handleMessage(new GenericMessage<String>("String content: Test AWS advice chain"));
		assertEquals(1, adviceCalled);
		ctx.destroy();
	}
	public static class FooAdvice extends AbstractRequestHandlerAdvice {
		@Override
		protected Object doInvoke(ExecutionCallback callback, Object target, Message<?> message) throws Exception {
			adviceCalled++;
			return callback.execute();
		}

	}
}
