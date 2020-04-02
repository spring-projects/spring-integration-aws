/*
 * Copyright 2016-2020 the original author or authors.
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

import org.springframework.cloud.aws.core.env.ResourceIdResolver;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.file.remote.session.SharedSessionCapable;
import org.springframework.util.Assert;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * An Amazon S3 specific {@link SessionFactory} implementation. Also this class implements
 * {@link SharedSessionCapable} around the single instance, since the {@link S3Session} is
 * simple thread-safe wrapper for the {@link AmazonS3}.
 *
 * @author Artem Bilan
 * @author Xavier François
 */
public class S3SessionFactory implements SessionFactory<S3ObjectSummary>, SharedSessionCapable {

	private final S3Session s3Session;

	public S3SessionFactory() {
		this(AmazonS3ClientBuilder.defaultClient());
	}

	public S3SessionFactory(AmazonS3 amazonS3) {
		this(amazonS3, null);
	}

	public S3SessionFactory(AmazonS3 amazonS3, ResourceIdResolver resourceIdResolver) {
		Assert.notNull(amazonS3, "'amazonS3' must not be null.");
		this.s3Session = new S3Session(amazonS3, resourceIdResolver);
	}

	@Override
	public S3Session getSession() {
		return this.s3Session;
	}

	@Override
	public boolean isSharedSession() {
		return true;
	}

	@Override
	public void resetSharedSession() {
		// No-op. The S3Session is stateless and can be used concurrently.
	}

	public void setEndpoint(String endpoint) {
		this.s3Session.setEndpoint(endpoint);
	}
}
