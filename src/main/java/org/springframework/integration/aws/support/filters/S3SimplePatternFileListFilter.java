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

import org.springframework.integration.file.filters.AbstractSimplePatternFileListFilter;

/**
 * Implementation of {@link AbstractSimplePatternFileListFilter} for Amazon S3.
 *
 * @author Artem Bilan
 */
public class S3SimplePatternFileListFilter extends AbstractSimplePatternFileListFilter<S3ObjectSummary> {

	public S3SimplePatternFileListFilter(String pattern) {
		super(pattern);
	}

	@Override
	protected String getFilename(S3ObjectSummary file) {
		return (file != null) ? file.getKey() : null;
	}

	@Override
	protected boolean isDirectory(S3ObjectSummary file) {
		return false;
	}

}
