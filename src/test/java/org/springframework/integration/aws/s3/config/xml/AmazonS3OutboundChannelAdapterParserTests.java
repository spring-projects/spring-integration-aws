/*
 * Copyright 2002-2015 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.springframework.integration.test.util.TestUtils.getPropertyValue;

import java.net.URI;

import org.junit.Test;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.integration.aws.s3.AmazonS3MessageHandler;
import org.springframework.integration.aws.s3.FileNameGenerationStrategy;
import org.springframework.integration.aws.s3.core.AmazonS3Operations;
import org.springframework.integration.aws.s3.core.DefaultAmazonS3Operations;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.handler.advice.AbstractRequestHandlerAdvice;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.GenericMessage;

/**
 * The test case for the aws-s3 namespace's {@link AmazonS3OutboundChannelAdapterParser} class
 * @author Amol Nayak
 * @author Rob Harrop
 * @author Karthikeyan Palanivelu
 * @since 0.5
 */
public class AmazonS3OutboundChannelAdapterParserTests {

	private volatile static int adviceCalled;

	/**
	 * Test case for the xml definition with a custom implementation of {@link AmazonS3Operations}
	 */
	@Test
	public void withCustomOperations() {
		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:s3-valid-outbound-cases.xml");
		EventDrivenConsumer consumer = ctx.getBean("withCustomService", EventDrivenConsumer.class);
		AmazonS3MessageHandler handler = getPropertyValue(consumer, "handler", AmazonS3MessageHandler.class);
		assertEquals(AmazonS3DummyOperations.class, getPropertyValue(handler, "operations").getClass());
		Expression expression =
				getPropertyValue(handler, "remoteDirectoryProcessor.expression", Expression.class);
		assertNotNull(expression);
		assertEquals(LiteralExpression.class, expression.getClass());
		assertEquals("/", getPropertyValue(expression, "literalValue", String.class));
		ctx.close();
	}

	/**
	 * Test case for the xml definition with the default implementation of {@link AmazonS3Operations}
	 */
	@Test
	public void withDefaultOperationsImplementation() {
		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:s3-valid-outbound-cases.xml");
		EventDrivenConsumer consumer = ctx.getBean("withDefaultServices", EventDrivenConsumer.class);
		AmazonS3MessageHandler handler = getPropertyValue(consumer, "handler", AmazonS3MessageHandler.class);
		assertEquals(DefaultAmazonS3Operations.class, getPropertyValue(handler, "operations").getClass());
		Expression expression =
				getPropertyValue(handler, "remoteDirectoryProcessor.expression", Expression.class);
		assertNotNull(expression);
		assertEquals(SpelExpression.class, expression.getClass());
		assertEquals("headers['remoteDirectory']", getPropertyValue(expression, "expression", String.class));
		assertEquals("TestBucket", getPropertyValue(handler, "bucket", String.class));
		assertEquals("US-ASCII", getPropertyValue(handler, "charset", String.class));
		assertEquals("dummy", getPropertyValue(handler, "credentials.accessKey", String.class));
		assertEquals("dummy", getPropertyValue(handler, "credentials.secretKey", String.class));
		assertEquals("dummy", getPropertyValue(handler, "operations.credentials.accessKey", String.class));
		assertEquals("dummy", getPropertyValue(handler, "operations.credentials.secretKey", String.class));
		assertEquals(5120, getPropertyValue(handler, "operations.multipartUploadThreshold", Long.class).longValue());
		assertEquals(".write", getPropertyValue(handler, "operations.temporaryFileSuffix", String.class));
		assertEquals(".write", getPropertyValue(handler, "fileNameGenerator.temporarySuffix", String.class));
		assertEquals("headers['name']", getPropertyValue(handler, "fileNameGenerator.fileNameExpression", String.class));
		assertEquals(ctx.getBean("executor"), getPropertyValue(handler, "operations.threadPoolExecutor"));
		ctx.close();
	}

	/**
	 * Test case for the xml definition with a custom implementation of {@link FileNameGenerationStrategy}
	 */
	@Test
	public void withCustomNameGenerator() {
		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("s3-valid-outbound-cases.xml");
		EventDrivenConsumer consumer = ctx.getBean("withCustomNameGenerator", EventDrivenConsumer.class);
		AmazonS3MessageHandler handler = getPropertyValue(consumer, "handler", AmazonS3MessageHandler.class);
		assertEquals(DummyFileNameGenerator.class, getPropertyValue(handler, "fileNameGenerator").getClass());
		ctx.close();
	}

	/**
	 * Test case for the xml definition with a custom AWS endpoint
	 */
	@Test
	public void withCustomEndpoint() {
		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("s3-valid-outbound-cases.xml");
		EventDrivenConsumer consumer = ctx.getBean("withCustomEndpoint", EventDrivenConsumer.class);
		AmazonS3MessageHandler handler = getPropertyValue(consumer, "handler", AmazonS3MessageHandler.class);
		assertEquals("http://s3-eu-west-1.amazonaws.com",
				getPropertyValue(handler, "operations.client.endpoint", URI.class).toString());
		ctx.close();
	}

	/**
	 * Multi part upload should have a size of 5120 and above, any value less than 5120 will
	 * thrown an exception
	 */
	@Test(expected = BeanCreationException.class)
	public void withMultiUploadLessThan5120() {
		new ClassPathXmlApplicationContext("s3-multiupload-lessthan-5120.xml").close();
	}

	/**
	 * Test with both the custom file generator and expression attribute set.
	 */
	@Test(expected = BeanDefinitionStoreException.class)
	public void withBothFileGeneratorAndExpression() {
		new ClassPathXmlApplicationContext("s3-both-customfilegenerator-and-expression.xml").close();
	}

	/**
	 * When custom implementation of {@link AmazonS3Operations} is provided, the attributes
	 * multipart-upload-threshold, temporary-directory, temporary-suffix and thread-pool-executor
	 * are not allowed
	 */
	@Test(expected = BeanDefinitionStoreException.class)
	public void withCustomOperationsAndDisallowedAttributes() {
		new ClassPathXmlApplicationContext("s3-custom-operations-with-disallowed-attributes.xml").close();
	}

	/**
	 * Tests the outbound channel adapter definition with a valid combination of attributes along with
	 * request handler chain.
	 */
	@Test
	public void withHandlerAdviceChain() {
		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("s3-valid-outbound-cases.xml");
		EventDrivenConsumer consumer = ctx.getBean("withHandlerChain", EventDrivenConsumer.class);
		MessageHandler handler = getPropertyValue(consumer, "handler", MessageHandler.class);
		handler.handleMessage(new GenericMessage<String>("String content: Test AWS advice chain"));
		assertEquals(1, adviceCalled);
		ctx.close();
	}

	/**
	 * Test case for the xml definition with the Channel Attributes.
	 */
	@Test
	public void withChannelAttributeImplementation() {
		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:s3-valid-outbound-cases.xml");
		EventDrivenConsumer consumer = ctx.getBean("withChannelAdapterAttributes", EventDrivenConsumer.class);
		AmazonS3MessageHandler handler = getPropertyValue(consumer, "handler", AmazonS3MessageHandler.class);
		assertEquals(DefaultAmazonS3Operations.class, getPropertyValue(handler, "operations").getClass());
		assertEquals("input", getPropertyValue(consumer, "inputChannel.beanName", String.class));
		assertEquals("US-ASCII", getPropertyValue(handler, "charset", String.class));
		assertEquals(100, getPropertyValue(consumer, "phase"));
		assertFalse(getPropertyValue(consumer, "autoStartup", Boolean.class));
		ctx.close();
	}

	public static class DummyFileNameGenerator implements FileNameGenerationStrategy {

		@Override
		public String generateFileName(Message<?> message) {
			return null;
		}

	}

	public static class FooAdvice extends AbstractRequestHandlerAdvice {

		@Override
		protected Object doInvoke(ExecutionCallback callback, Object target, Message<?> message) throws Exception {
			adviceCalled++;
			return callback.execute();
		}

	}

}
