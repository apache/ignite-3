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

package org.apache.ignite.internal.configuration.exception;

import static org.apache.ignite.lang.ErrorGroups.CommonConfiguration.CONFIGURATION_APPLY_ERR;

import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Throws when failed to apply new configuration.
 */
public class ConfigurationApplyException extends IgniteException {
    private static final long serialVersionUID = -3564943126391592841L;

    public ConfigurationApplyException(String message) {
        super(CONFIGURATION_APPLY_ERR, message);
    }

    @SuppressWarnings("PMD.UnusedFormalParameter")
    public ConfigurationApplyException(String message, @Nullable Throwable cause) {
        super(CONFIGURATION_APPLY_ERR, cause);
    }
}
