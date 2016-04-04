/*
 * Copyright 2016 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.expression.Expression;
import org.springframework.integration.aws.inbound.S3InboundFileSynchronizer;
import org.springframework.integration.aws.inbound.S3InboundFileSynchronizingMessageSource;
import org.springframework.integration.aws.support.filters.S3SimplePatternFileListFilter;
import org.springframework.integration.endpoint.SourcePollingChannelAdapter;
import org.springframework.integration.file.filters.AcceptAllFileListFilter;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.file.remote.synchronizer.AbstractInboundFileSynchronizer;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ReflectionUtils;

/**
 * @author Artem Bilan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class S3InboundChannelAdapterParserTests {

	@Autowired
	private SourcePollingChannelAdapter s3Inbound;

	@Autowired
	private Comparator<?> comparator;

	@Autowired
	private MessageChannel s3Channel;

	@Autowired
	private AcceptAllFileListFilter<?> acceptAllFilter;

	@Autowired
	private SessionFactory<?> s3SessionFactory;

	@Test
	public void testFtpInboundChannelAdapterComplete() throws Exception {
		assertFalse(TestUtils.getPropertyValue(this.s3Inbound, "autoStartup", Boolean.class));
		PriorityBlockingQueue<?> blockingQueue = TestUtils.getPropertyValue(this.s3Inbound,
				"source.fileSource.toBeReceived", PriorityBlockingQueue.class);
		Comparator<?> comparator = blockingQueue.comparator();
		assertSame(this.comparator, comparator);
		assertEquals("s3Inbound", this.s3Inbound.getComponentName());
		assertEquals("aws:s3-inbound-channel-adapter", this.s3Inbound.getComponentType());
		assertSame(this.s3Channel, TestUtils.getPropertyValue(this.s3Inbound, "outputChannel"));

		S3InboundFileSynchronizingMessageSource inbound = TestUtils.getPropertyValue(this.s3Inbound, "source",
				S3InboundFileSynchronizingMessageSource.class);

		S3InboundFileSynchronizer fisync = TestUtils.getPropertyValue(inbound, "synchronizer",
				S3InboundFileSynchronizer.class);
		assertEquals("'foo/bar'",
				TestUtils.getPropertyValue(fisync, "remoteDirectoryExpression", Expression.class)
						.getExpressionString());
		assertNotNull(TestUtils.getPropertyValue(fisync, "localFilenameGeneratorExpression"));
		assertTrue(TestUtils.getPropertyValue(fisync, "preserveTimestamp", Boolean.class));
		assertEquals(".foo", TestUtils.getPropertyValue(fisync, "temporaryFileSuffix", String.class));
		String remoteFileSeparator = (String) TestUtils.getPropertyValue(fisync, "remoteFileSeparator");
		assertNotNull(remoteFileSeparator);
		assertEquals("", remoteFileSeparator);
		S3SimplePatternFileListFilter filter = TestUtils.getPropertyValue(fisync, "filter",
				S3SimplePatternFileListFilter.class);
		assertNotNull(filter);
		assertSame(this.s3SessionFactory, TestUtils.getPropertyValue(fisync, "remoteFileTemplate.sessionFactory"));
		assertTrue(TestUtils.getPropertyValue(inbound, "fileSource.scanner.filter.fileFilters", Collection.class)
				.contains(this.acceptAllFilter));
		final AtomicReference<Method> genMethod = new AtomicReference<Method>();
		ReflectionUtils.doWithMethods(AbstractInboundFileSynchronizer.class, new ReflectionUtils.MethodCallback() {

			@Override
			public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
				if ("generateLocalFileName".equals(method.getName())) {
					method.setAccessible(true);
					genMethod.set(method);
				}
			}

		});
		assertEquals("FOO.afoo", genMethod.get().invoke(fisync, "foo"));
	}

}
