/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.raft.configuration;

import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockValidationContext;
import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.validate;
import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.DEFAULT_MAX_CHECKPOINT_QUEUE_SIZE;
import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.DEFAULT_SEGMENT_FILE_SIZE_BYTES;
import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.UNSPECIFIED_MAX_LOG_ENTRY_SIZE;
import static org.mockito.Mockito.mock;

import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

class LogStorageConfigurationValidatorTest extends BaseIgniteAbstractTest {
    private final LogStorageConfigurationValidator validator = new LogStorageConfigurationValidator();

    @Test
    void unspecifiedLogEntrySizeIsValid() {
        var config = new MockLogStorageView(
                DEFAULT_MAX_CHECKPOINT_QUEUE_SIZE,
                DEFAULT_SEGMENT_FILE_SIZE_BYTES,
                UNSPECIFIED_MAX_LOG_ENTRY_SIZE
        );

        validate(
                validator,
                mock(ValidLogStorageConfiguration.class),
                mockValidationContext(null, config)
        );
    }

    @Test
    void correctLogEntrySizeIsValid() {
        var config = new MockLogStorageView(
                DEFAULT_MAX_CHECKPOINT_QUEUE_SIZE,
                DEFAULT_SEGMENT_FILE_SIZE_BYTES,
                (int) (DEFAULT_SEGMENT_FILE_SIZE_BYTES * 0.9)
        );

        validate(
                validator,
                mock(ValidLogStorageConfiguration.class),
                mockValidationContext(null, config)
        );
    }

    @Test
    void zeroLogEntrySizeIsNotValid() {
        var config = new MockLogStorageView(
                DEFAULT_MAX_CHECKPOINT_QUEUE_SIZE,
                DEFAULT_SEGMENT_FILE_SIZE_BYTES,
                0
        );

        validate(
                validator,
                mock(ValidLogStorageConfiguration.class),
                mockValidationContext(null, config),
                "Maximum log entry size must be positive, got 0."
        );
    }

    @Test
    void logEntrySizeEqualToSegmentFileSizeIsNotValid() {
        var config = new MockLogStorageView(
                DEFAULT_MAX_CHECKPOINT_QUEUE_SIZE,
                10,
                10
        );

        validate(
                validator,
                mock(ValidLogStorageConfiguration.class),
                mockValidationContext(null, config),
                "Maximum log entry size is too big (10 bytes), maximum allowed log entry size is 9 bytes."
        );
    }

    private static class MockLogStorageView implements LogStorageView {
        private final int maxCheckpointQueueSize;

        private final int segmentFileSizeBytes;

        private final int maxLogEntrySizeBytes;

        MockLogStorageView(int maxCheckpointQueueSize, int segmentFileSizeBytes, int maxLogEntrySizeBytes) {
            this.maxCheckpointQueueSize = maxCheckpointQueueSize;
            this.segmentFileSizeBytes = segmentFileSizeBytes;
            this.maxLogEntrySizeBytes = maxLogEntrySizeBytes;
        }

        @Override
        public int maxCheckpointQueueSize() {
            return maxCheckpointQueueSize;
        }

        @Override
        public long segmentFileSizeBytes() {
            return segmentFileSizeBytes;
        }

        @Override
        public int maxLogEntrySizeBytes() {
            return maxLogEntrySizeBytes;
        }
    }
}
