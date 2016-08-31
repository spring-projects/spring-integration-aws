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

package org.springframework.integration.aws.support;

import org.springframework.integration.file.remote.AbstractFileInfo;
import org.springframework.util.Assert;

import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * An Amazon S3 {@link org.springframework.integration.file.remote.FileInfo} implementation.
 * @author Christian Tzolov
 */
public class S3FileInfo extends AbstractFileInfo<S3ObjectSummary> {

	private final S3ObjectSummary s3ObjectSummary;

	public S3FileInfo(S3ObjectSummary s3ObjectSummary) {
		Assert.notNull(s3ObjectSummary, "s3ObjectSummary must not be null");
		this.s3ObjectSummary = s3ObjectSummary;
	}

	@Override
	public boolean isDirectory() {
		return false;
	}

	@Override
	public boolean isLink() {
		return false;
	}

	@Override
	public long getSize() {
		return s3ObjectSummary.getSize();
	}

	@Override
	public long getModified() {
		return s3ObjectSummary.getLastModified().getTime();
	}

	@Override
	public String getFilename() {
		return s3ObjectSummary.getKey();
	}

	@Override
	public String getPermissions() {
		return "-rw-r--r--";
	}

	@Override
	public S3ObjectSummary getFileInfo() {
		return s3ObjectSummary;
	}

}
