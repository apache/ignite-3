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
import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.DEFAULT_SEGMENT_FILE_SIZE_BYTES;
import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.UNSPECIFIED_MAX_LOG_ENTRY_SIZE;
import static org.apache.ignite.internal.util.ArrayUtils.STRING_EMPTY_ARRAY;

import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.validation.TestValidationUtil;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
class LogStorageConfigurationValidatorTest extends BaseIgniteAbstractTest {
    @Mock
    private static ValidLogStorageConfiguration VALID_LOG_STORAGE_CONFIGURATION;

    private final LogStorageConfigurationValidator validator = new LogStorageConfigurationValidator();

    @Test
    void unspecifiedLogEntrySizeIsValid(
            @InjectConfiguration(value = "mock.maxLogEntrySizeBytes=" + UNSPECIFIED_MAX_LOG_ENTRY_SIZE, validate = false)
            LogStorageConfiguration config
    ) {
        validate(config);
    }

    @Test
    void correctLogEntrySizeIsValid(
            @InjectConfiguration(value = "mock.maxLogEntrySizeBytes=" + (int) (DEFAULT_SEGMENT_FILE_SIZE_BYTES * 0.9), validate = false)
            LogStorageConfiguration config
    ) {
        validate(config);
    }

    @Test
    void zeroLogEntrySizeIsNotValid(
            @InjectConfiguration(value = "mock.maxLogEntrySizeBytes=0", validate = false)
            LogStorageConfiguration config
    ) {
        validate(config, "Maximum log entry size must be positive [maxEntrySize=0 bytes].");
    }

    @Test
    void logEntrySizeEqualToSegmentFileSizeIsNotValid(
            @InjectConfiguration(value = "mock { maxLogEntrySizeBytes=10, segmentFileSizeBytes=10 }", validate = false)
            LogStorageConfiguration config
    ) {
        validate(config, "Maximum log entry size is too big [maxEntrySize=10 bytes, maxAllowedEntrySize=9 bytes].");
    }

    @Test
    void softLimitLessThanSegmentFileSizeIsNotValid(
            @InjectConfiguration(value = "mock { softLogSizeLimitBytes=500, segmentFileSizeBytes=1000 }", validate = false)
            LogStorageConfiguration config
    ) {
        validate(config, "Soft log size limit must be at least the segment file size [softLimit=500 bytes, segmentFileSize=1000 bytes].");
    }

    @Test
    void softLimitEqualToSegmentFileSizeIsValid(
            @InjectConfiguration(value = "mock { softLogSizeLimitBytes=1000, segmentFileSizeBytes=1000 }", validate = false)
            LogStorageConfiguration config
    ) {
        validate(config);
    }

    private void validate(LogStorageConfiguration config) {
        validate(config, STRING_EMPTY_ARRAY);
    }

    private void validate(LogStorageConfiguration config, String @Nullable ... errorMessagePrefixes) {
        TestValidationUtil.validate(
                validator,
                VALID_LOG_STORAGE_CONFIGURATION,
                mockValidationContext(null, config.value()),
                errorMessagePrefixes
        );
    }
}
