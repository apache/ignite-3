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

import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.computeDefaultMaxLogEntrySizeBytes;

import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;

class LogStorageConfigurationValidator implements Validator<ValidLogStorageConfiguration, LogStorageView> {
    static final LogStorageConfigurationValidator INSTANCE = new LogStorageConfigurationValidator();

    @Override
    public void validate(ValidLogStorageConfiguration annotation, ValidationContext<LogStorageView> ctx) {
        LogStorageView newValue = ctx.getNewValue();

        validateLogEntrySize(newValue, ctx);
    }

    private static void validateLogEntrySize(LogStorageView config, ValidationContext<LogStorageView> ctx) {
        int maxEntrySize = config.maxLogEntrySizeBytes();

        if (maxEntrySize == LogStorageConfigurationSchema.UNSPECIFIED_MAX_LOG_ENTRY_SIZE) {
            return;
        }

        if (maxEntrySize <= 0) {
            String errorMsg = String.format("Maximum log entry size must be positive, got %d.", maxEntrySize);

            ctx.addIssue(new ValidationIssue(ctx.currentKey(), errorMsg));

            return;
        }

        int maxAllowedEntrySize = computeDefaultMaxLogEntrySizeBytes(config.segmentFileSizeBytes());

        if (maxEntrySize > maxAllowedEntrySize) {
            String errorMsg = String.format(
                    "Maximum log entry size is too big (%d bytes), maximum allowed log entry size is %d bytes.",
                    maxEntrySize, maxAllowedEntrySize
            );

            ctx.addIssue(new ValidationIssue(ctx.currentKey(), errorMsg));
        }
    }
}
