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

package org.springframework.integration.aws.support.filters;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.springframework.integration.file.filters.AbstractPersistentAcceptOnceFileListFilter;
import org.springframework.integration.metadata.ConcurrentMetadataStore;

/**
 * Persistent file list filter using the server's file timestamp to detect if we've
 * already 'seen' this file.
 *
 * @author Artem Bilan
 */
public class S3PersistentAcceptOnceFileListFilter extends AbstractPersistentAcceptOnceFileListFilter<S3ObjectSummary> {

	public S3PersistentAcceptOnceFileListFilter(ConcurrentMetadataStore store, String prefix) {
		super(store, prefix);
	}

	@Override
	protected long modified(S3ObjectSummary file) {
		return (file != null) ? file.getLastModified().getTime() : 0L;
	}

	@Override
	protected String fileName(S3ObjectSummary file) {
		return (file != null) ? file.getKey() : null;
	}

	/**
	 * Always return false since no directory notion in S3.
	 * @param file the {@link S3ObjectSummary}
	 * @return always false: S3 does not have a notion of directory
	 * @since 2.5
	 */
	@Override
	protected boolean isDirectory(S3ObjectSummary file) {
		return false;
	}

}
