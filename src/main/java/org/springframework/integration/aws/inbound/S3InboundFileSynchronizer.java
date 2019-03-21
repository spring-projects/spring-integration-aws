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

package org.springframework.integration.aws.inbound;

import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.aws.support.S3SessionFactory;
import org.springframework.integration.file.remote.session.Session;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.file.remote.synchronizer.AbstractInboundFileSynchronizer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * An implementation of {@link AbstractInboundFileSynchronizer} for Amazon S3.
 *
 * @author Artem Bilan
 */
public class S3InboundFileSynchronizer extends AbstractInboundFileSynchronizer<S3ObjectSummary> {

	public S3InboundFileSynchronizer() {
		this(new S3SessionFactory());
	}

	public S3InboundFileSynchronizer(AmazonS3 amazonS3) {
		this(new S3SessionFactory(amazonS3));
	}

	/**
	 * Create a synchronizer with the {@link SessionFactory} used to acquire {@link Session} instances.
	 * @param sessionFactory The session factory.
	 */
	public S3InboundFileSynchronizer(SessionFactory<S3ObjectSummary> sessionFactory) {
		super(sessionFactory);
		setRemoteDirectoryExpression(new LiteralExpression(null));
	}

	@Override
	protected boolean isFile(S3ObjectSummary file) {
		return true;
	}

	@Override
	protected String getFilename(S3ObjectSummary file) {
		return (file != null ? file.getKey() : null);
	}

	@Override
	protected long getModified(S3ObjectSummary file) {
		return file.getLastModified().getTime();
	}

}
