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

package org.springframework.integration.aws.support.filters;

import java.util.regex.Pattern;

import org.springframework.integration.file.filters.AbstractRegexPatternFileListFilter;

import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Implementation of {@link AbstractRegexPatternFileListFilter} for Amazon S3.
 *
 * @author Artem Bilan
 */
public class S3RegexPatternFileListFilter extends AbstractRegexPatternFileListFilter<S3ObjectSummary> {

	public S3RegexPatternFileListFilter(String pattern) {
		super(pattern);
	}

	public S3RegexPatternFileListFilter(Pattern pattern) {
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
