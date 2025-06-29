/*
 * Copyright 2017-present the original author or authors.
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

package org.springframework.integration.aws.inbound.kinesis;

import org.springframework.messaging.Message;

/**
 * The listener mode, record or batch.
 *
 * @author Artem Bilan
 *
 * @since 1.1
 */
public enum ListenerMode {

	/**
	 * Each {@link Message} will be converted from a single {@code Record}.
	 */
	record,

	/**
	 * Each {@link Message} will contain {@code List} ( if not empty) of converted or raw
	 * {@code Record}s.
	 */
	batch

}
