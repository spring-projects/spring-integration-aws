/*
 * Copyright 2016-2021 the original author or authors.
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

import java.io.File;
import java.io.IOException;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.aws.support.S3Session;
import org.springframework.integration.aws.support.S3SessionFactory;
import org.springframework.integration.aws.support.filters.S3PersistentAcceptOnceFileListFilter;
import org.springframework.integration.file.remote.session.Session;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.file.remote.synchronizer.AbstractInboundFileSynchronizer;
import org.springframework.integration.metadata.SimpleMetadataStore;
import org.springframework.lang.Nullable;

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
	 * Create a synchronizer with the {@link SessionFactory} used to acquire
	 * {@link Session} instances.
	 * @param sessionFactory The session factory.
	 */
	public S3InboundFileSynchronizer(SessionFactory<S3ObjectSummary> sessionFactory) {
		super(sessionFactory);
		doSetRemoteDirectoryExpression(new LiteralExpression(null));
		doSetFilter(new S3PersistentAcceptOnceFileListFilter(new SimpleMetadataStore(), "s3MessageSource"));
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

	@Override
	protected boolean copyFileToLocalDirectory(String remoteDirectoryPath,
			@Nullable EvaluationContext localFileEvaluationContext, S3ObjectSummary remoteFile,
			File localDirectory, Session<S3ObjectSummary> session) throws IOException {

		return super.copyFileToLocalDirectory(((S3Session) session).normalizeBucketName(remoteDirectoryPath),
				localFileEvaluationContext, remoteFile, localDirectory, session);
	}

	@Override
	protected String protocol() {
		return "s3";
	}

}
