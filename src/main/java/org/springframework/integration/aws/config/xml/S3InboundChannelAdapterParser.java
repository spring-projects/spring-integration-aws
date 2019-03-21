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

package org.springframework.integration.aws.config.xml;

import org.springframework.integration.aws.inbound.S3InboundFileSynchronizer;
import org.springframework.integration.aws.inbound.S3InboundFileSynchronizingMessageSource;
import org.springframework.integration.aws.support.filters.S3RegexPatternFileListFilter;
import org.springframework.integration.aws.support.filters.S3SimplePatternFileListFilter;
import org.springframework.integration.file.config.AbstractRemoteFileInboundChannelAdapterParser;

/**
 * Parser for the AWS 's3-inbound-channel-adapter' element.
 *
 * @author Artem Bilan
 */
public class S3InboundChannelAdapterParser extends AbstractRemoteFileInboundChannelAdapterParser {

	@Override
	protected String getMessageSourceClassname() {
		return S3InboundFileSynchronizingMessageSource.class.getName();
	}

	@Override
	protected String getInboundFileSynchronizerClassname() {
		return S3InboundFileSynchronizer.class.getName();
	}

	@Override
	protected String getSimplePatternFileListFilterClassname() {
		return S3SimplePatternFileListFilter.class.getName();
	}

	@Override
	protected String getRegexPatternFileListFilterClassname() {
		return S3RegexPatternFileListFilter.class.getName();
	}

}
