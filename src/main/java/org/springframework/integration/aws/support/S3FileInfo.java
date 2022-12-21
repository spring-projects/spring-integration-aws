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

import java.util.Date;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.springframework.integration.file.remote.AbstractFileInfo;
import org.springframework.util.Assert;

/**
 * An Amazon S3 {@link org.springframework.integration.file.remote.FileInfo}
 * implementation.
 *
 * @author Christian Tzolov
 * @since 1.1
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
		return this.s3ObjectSummary.getSize();
	}

	@Override
	public long getModified() {
		return this.s3ObjectSummary.getLastModified().getTime();
	}

	@Override
	public String getFilename() {
		return this.s3ObjectSummary.getKey();
	}

	/**
	 * A permissions representation string. Throws {@link UnsupportedOperationException}
	 * to avoid extra {@link com.amazonaws.services.s3.AmazonS3#getObjectAcl} REST call.
	 * The target application amy choose to do that by its logic.
	 * @return the permissions representation string.
	 */
	@Override
	public String getPermissions() {
		throw new UnsupportedOperationException("Use [AmazonS3.getObjectAcl()] to obtain permissions.");
	}

	@Override
	public S3ObjectSummary getFileInfo() {
		return this.s3ObjectSummary;
	}

	@Override
	public String toString() {
		return "FileInfo [isDirectory=" + isDirectory() + ", isLink=" + isLink() + ", Size=" + getSize()
				+ ", ModifiedTime=" + new Date(getModified()) + ", Filename=" + getFilename() + ", RemoteDirectory="
				+ getRemoteDirectory() + "]";
	}

}
