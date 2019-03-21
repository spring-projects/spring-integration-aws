/*
 * Copyright 2016 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

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
		assertThat(TestUtils.getPropertyValue(this.s3Inbound, "autoStartup", Boolean.class)).isFalse();
		PriorityBlockingQueue<?> blockingQueue = TestUtils.getPropertyValue(this.s3Inbound,
				"source.fileSource.toBeReceived", PriorityBlockingQueue.class);
		Comparator<?> comparator = blockingQueue.comparator();
		assertThat(comparator).isSameAs(this.comparator);
		assertThat(this.s3Inbound.getComponentName()).isEqualTo("s3Inbound");
		assertThat(this.s3Inbound.getComponentType()).isEqualTo("aws:s3-inbound-channel-adapter");
		assertThat(TestUtils.getPropertyValue(this.s3Inbound, "outputChannel")).isSameAs(this.s3Channel);

		S3InboundFileSynchronizingMessageSource inbound = TestUtils.getPropertyValue(this.s3Inbound, "source",
				S3InboundFileSynchronizingMessageSource.class);

		S3InboundFileSynchronizer fisync = TestUtils.getPropertyValue(inbound, "synchronizer",
				S3InboundFileSynchronizer.class);
		assertThat(TestUtils.getPropertyValue(fisync, "remoteDirectoryExpression", Expression.class)
				.getExpressionString())
				.isEqualTo("'foo/bar'");
		assertThat(TestUtils.getPropertyValue(fisync, "localFilenameGeneratorExpression")).isNotNull();
		assertThat(TestUtils.getPropertyValue(fisync, "preserveTimestamp", Boolean.class)).isTrue();
		assertThat(TestUtils.getPropertyValue(fisync, "temporaryFileSuffix", String.class)).isEqualTo(".foo");
		String remoteFileSeparator = (String) TestUtils.getPropertyValue(fisync, "remoteFileSeparator");
		assertThat(remoteFileSeparator).isNotNull();
		assertThat(remoteFileSeparator).isEmpty();
		S3SimplePatternFileListFilter filter = TestUtils.getPropertyValue(fisync, "filter",
				S3SimplePatternFileListFilter.class);
		assertThat(filter).isNotNull();
		assertThat(TestUtils.getPropertyValue(fisync, "remoteFileTemplate.sessionFactory"))
				.isSameAs(this.s3SessionFactory);
		assertThat(TestUtils.getPropertyValue(inbound, "fileSource.scanner.filter.fileFilters", Collection.class)
				.contains(this.acceptAllFilter))
				.isTrue();
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
		assertThat(genMethod.get().invoke(fisync, "foo")).isEqualTo("FOO.afoo");
	}

}
