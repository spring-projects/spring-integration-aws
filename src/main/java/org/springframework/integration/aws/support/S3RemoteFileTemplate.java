/*
 * Copyright 2016-2022 the original author or authors.
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

import java.io.IOException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.springframework.integration.file.remote.ClientCallback;
import org.springframework.integration.file.remote.RemoteFileTemplate;
import org.springframework.integration.file.remote.session.SessionFactory;

/**
 * An Amazon S3 specific {@link RemoteFileTemplate} extension.
 *
 * @author Artem Bilan
 */
public class S3RemoteFileTemplate extends RemoteFileTemplate<S3ObjectSummary> {

	public S3RemoteFileTemplate() {
		this(new S3SessionFactory());
	}

	public S3RemoteFileTemplate(AmazonS3 amazonS3) {
		this(new S3SessionFactory(amazonS3));
	}

	/**
	 * Construct a {@link RemoteFileTemplate} with the supplied session factory.
	 * @param sessionFactory the session factory.
	 */
	public S3RemoteFileTemplate(SessionFactory<S3ObjectSummary> sessionFactory) {
		super(sessionFactory);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T, C> T executeWithClient(final ClientCallback<C, T> callback) {
		return callback.doWithClient((C) this.sessionFactory.getSession().getClientInstance());
	}

	@Override
	public boolean exists(final String path) {
		try {
			return this.sessionFactory.getSession().exists(path);
		}
		catch (IOException e) {
			throw new AmazonS3Exception("Failed to check the path " + path, e);
		}
	}

}
