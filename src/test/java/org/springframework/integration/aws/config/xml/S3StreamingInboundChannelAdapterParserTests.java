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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.expression.Expression;
import org.springframework.integration.aws.inbound.S3StreamingMessageSource;
import org.springframework.integration.aws.support.filters.S3PersistentAcceptOnceFileListFilter;
import org.springframework.integration.endpoint.SourcePollingChannelAdapter;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.file.remote.synchronizer.AbstractInboundFileSynchronizer;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.ReflectionUtils;

/**
 * @author Christian Tzolov
 * @author Artem Bilan
 */
@SpringJUnitConfig
@DirtiesContext
class S3StreamingInboundChannelAdapterParserTests {

	@Autowired
	private SourcePollingChannelAdapter s3Inbound;

	@Autowired
	private Comparator<?> comparator;

	@Autowired
	private MessageChannel s3Channel;

	@Autowired
	private S3PersistentAcceptOnceFileListFilter acceptOnceFilter;

	@Autowired
	private SessionFactory<?> s3SessionFactory;

	@Test
	void testS3StreamingInboundChannelAdapterComplete() {

		assertThat(TestUtils.getPropertyValue(this.s3Inbound, "autoStartup", Boolean.class)).isFalse();
		assertThat(this.s3Inbound.getComponentName()).isEqualTo("s3Inbound");
		assertThat(this.s3Inbound.getComponentType()).isEqualTo("aws:s3-inbound-streaming-channel-adapter");
		assertThat(TestUtils.getPropertyValue(this.s3Inbound, "outputChannel")).isSameAs(this.s3Channel);

		S3StreamingMessageSource source = TestUtils.getPropertyValue(this.s3Inbound, "source",
				S3StreamingMessageSource.class);

		assertThat(
				TestUtils.getPropertyValue(source, "remoteDirectoryExpression", Expression.class).getExpressionString())
				.isEqualTo("foo/bar");

		assertThat(TestUtils.getPropertyValue(source, "comparator")).isSameAs(this.comparator);
		String remoteFileSeparator = (String) TestUtils.getPropertyValue(source, "remoteFileSeparator");
		assertThat(remoteFileSeparator).isNotNull();
		assertThat(remoteFileSeparator).isEqualTo("\\");

		S3PersistentAcceptOnceFileListFilter filter = TestUtils.getPropertyValue(source, "filter",
				S3PersistentAcceptOnceFileListFilter.class);
		assertThat(filter).isSameAs(this.acceptOnceFilter);
		assertThat(TestUtils.getPropertyValue(source, "remoteFileTemplate.sessionFactory"))
				.isSameAs(this.s3SessionFactory);

		final AtomicReference<Method> genMethod = new AtomicReference<Method>();
		ReflectionUtils.doWithMethods(AbstractInboundFileSynchronizer.class,
				method -> {
					if ("generateLocalFileName".equals(method.getName())) {
						method.setAccessible(true);
						genMethod.set(method);
					}
				});
	}

}
