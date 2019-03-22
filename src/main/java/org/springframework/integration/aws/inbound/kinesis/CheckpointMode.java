/*
 * Copyright 2017 the original author or authors.
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

/**
 * The listener mode, record or batch.
 *
 * @author Artem Bilan
 *
 * @since 1.1
 */
public enum CheckpointMode {

	/**
	 * Checkpoint after each processed record.
	 * Makes sense only if {@link ListenerMode#record} is used.
	 */
	record,

	/**
	 * Checkpoint after each processed batch of records.
	 */
	batch,

	/**
	 * Checkpoint on demand via provided to the message {@link Checkpointer} callback.
	 */
	manual

}
