/*
 * Copyright 2017-2018 the original author or authors.
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

package org.springframework.integration.aws.inbound.kinesis;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.integration.metadata.MetadataStore;

import com.amazonaws.services.kinesis.model.Record;

/**
 * An internal {@link Checkpointer} implementation based on
 * provided {@link MetadataStore} and {@code key} for shard.
 * <p>
 * The instances of this class is created by the {@link KinesisMessageDrivenChannelAdapter}
 * for each {@code ShardConsumer}.
 *
 * @author Artem Bilan
 *
 * @since 1.1
 */
class ShardCheckpointer implements Checkpointer {

	private static final Log logger = LogFactory.getLog(ShardCheckpointer.class);

	private final MetadataStore checkpointStore;

	private final String key;

	private volatile String lastCheckpointValue;

	private volatile boolean active = true;

	ShardCheckpointer(MetadataStore checkpointStore, String key) {
		this.checkpointStore = checkpointStore;
		this.key = key;
	}

	@Override
	public boolean checkpoint() {
		return checkpoint(this.lastCheckpointValue);
	}

	@Override
	public boolean checkpoint(String sequenceNumber) {
		if (this.active) {
			String existingSequence = this.checkpointStore.get(this.key);
			if (existingSequence == null ||
					new BigInteger(existingSequence).compareTo(new BigInteger(sequenceNumber)) < 0) {
				this.checkpointStore.put(this.key, sequenceNumber);
				return true;
			}
		}
		else {
			if (logger.isInfoEnabled()) {
				logger.info("The [" + this + "] has been closed. Checkpoints aren't accepted anymore.");
			}
		}

		return false;
	}

	List<Record> filterRecords(List<Record> records) {
		List<Record> recordsToProcess = new LinkedList<>(records);
		this.lastCheckpointValue = getCheckpoint();
		if (this.lastCheckpointValue != null) {
			for (Iterator<Record> iterator = recordsToProcess.iterator(); iterator.hasNext(); ) {
				Record record = iterator.next();
				String sequenceNumber = record.getSequenceNumber();
				if (new BigInteger(sequenceNumber).compareTo(new BigInteger(this.lastCheckpointValue)) <= 0) {
					if (logger.isDebugEnabled()) {
						logger.debug("Removing record with sequenceNumber " + sequenceNumber +
								" because the sequenceNumber is <= checkpoint(" + this.lastCheckpointValue + ")");
					}
					iterator.remove();
				}
				else {
					this.lastCheckpointValue = sequenceNumber;
				}
			}

		}
		else {
			this.lastCheckpointValue = recordsToProcess.get(recordsToProcess.size() - 1).getSequenceNumber();
		}
		return recordsToProcess;
	}

	String getCheckpoint() {
		return this.checkpointStore.get(this.key);
	}

	String getLastCheckpointValue() {
		return this.lastCheckpointValue;
	}

	void remove() {
		this.checkpointStore.remove(this.key);
	}

	void close() {
		this.active = false;
	}

	@Override
	public String toString() {
		return "ShardCheckpointer{" +
				"key='" + this.key + '\'' +
				", lastCheckpointValue='" + this.lastCheckpointValue + '\'' +
				'}';
	}

}
