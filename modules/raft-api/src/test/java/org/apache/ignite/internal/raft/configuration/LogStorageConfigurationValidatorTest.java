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
import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.DEFAULT_MAX_CHECKPOINT_QUEUE_SIZE;
import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.DEFAULT_SEGMENT_FILE_SIZE_BYTES;
import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.DEFAULT_SOFT_LOG_SIZE_LIMIT_BYTES;
import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.UNSPECIFIED_MAX_LOG_ENTRY_SIZE;
import static org.apache.ignite.internal.util.ArrayUtils.STRING_EMPTY_ARRAY;

import org.apache.ignite.internal.configuration.validation.TestValidationUtil;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LogStorageConfigurationValidatorTest extends BaseIgniteAbstractTest {
    @Mock
    private static ValidLogStorageConfiguration VALID_LOG_STORAGE_CONFIGURATION;

    private final LogStorageConfigurationValidator validator = new LogStorageConfigurationValidator();

    @Test
    void unspecifiedLogEntrySizeIsValid() {
        var config = new MockLogStorageView(
                DEFAULT_MAX_CHECKPOINT_QUEUE_SIZE,
                DEFAULT_SEGMENT_FILE_SIZE_BYTES,
                UNSPECIFIED_MAX_LOG_ENTRY_SIZE,
                DEFAULT_SOFT_LOG_SIZE_LIMIT_BYTES
        );

        validate(config);
    }

    @Test
    void correctLogEntrySizeIsValid() {
        var config = new MockLogStorageView(
                DEFAULT_MAX_CHECKPOINT_QUEUE_SIZE,
                DEFAULT_SEGMENT_FILE_SIZE_BYTES,
                (int) (DEFAULT_SEGMENT_FILE_SIZE_BYTES * 0.9),
                DEFAULT_SOFT_LOG_SIZE_LIMIT_BYTES
        );

        validate(config);
    }

    @Test
    void zeroLogEntrySizeIsNotValid() {
        var config = new MockLogStorageView(
                DEFAULT_MAX_CHECKPOINT_QUEUE_SIZE,
                DEFAULT_SEGMENT_FILE_SIZE_BYTES,
                0,
                DEFAULT_SOFT_LOG_SIZE_LIMIT_BYTES
        );

        validate(config, "Maximum log entry size must be positive [maxEntrySize=0 bytes].");
    }

    @Test
    void logEntrySizeEqualToSegmentFileSizeIsNotValid() {
        var config = new MockLogStorageView(
                DEFAULT_MAX_CHECKPOINT_QUEUE_SIZE,
                10,
                10,
                DEFAULT_SOFT_LOG_SIZE_LIMIT_BYTES
        );

        validate(config, "Maximum log entry size is too big [maxEntrySize=10 bytes, maxAllowedEntrySize=9 bytes].");
    }

    @Test
    void softLimitLessThanSegmentFileSizeIsNotValid() {
        var config = new MockLogStorageView(
                DEFAULT_MAX_CHECKPOINT_QUEUE_SIZE,
                1000,
                UNSPECIFIED_MAX_LOG_ENTRY_SIZE,
                500
        );

        validate(config, "Soft log size limit must be at least the segment file size [softLimit=500 bytes, segmentFileSize=1000 bytes].");
    }

    @Test
    void softLimitEqualToSegmentFileSizeIsValid() {
        var config = new MockLogStorageView(
                DEFAULT_MAX_CHECKPOINT_QUEUE_SIZE,
                1000,
                UNSPECIFIED_MAX_LOG_ENTRY_SIZE,
                1000
        );

        validate(config);
    }

    private void validate(LogStorageView config) {
        validate(config, STRING_EMPTY_ARRAY);
    }

    private void validate(LogStorageView config, String @Nullable ... errorMessagePrefixes) {
        TestValidationUtil.validate(
                validator,
                VALID_LOG_STORAGE_CONFIGURATION,
                mockValidationContext(null, config),
                errorMessagePrefixes
        );
    }

    private static class MockLogStorageView implements LogStorageView {
        private final int maxCheckpointQueueSize;

        private final long segmentFileSizeBytes;

        private final int maxLogEntrySizeBytes;

        private final long softLogSizeLimitBytes;

        MockLogStorageView(int maxCheckpointQueueSize, long segmentFileSizeBytes, int maxLogEntrySizeBytes,
                long softLogSizeLimitBytes) {
            this.maxCheckpointQueueSize = maxCheckpointQueueSize;
            this.segmentFileSizeBytes = segmentFileSizeBytes;
            this.maxLogEntrySizeBytes = maxLogEntrySizeBytes;
            this.softLogSizeLimitBytes = softLogSizeLimitBytes;
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

        @Override
        public long softLogSizeLimitBytes() {
            return softLogSizeLimitBytes;
        }
    }
}
