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

package org.apache.ignite.client;

import static org.apache.ignite.lang.ErrorGroups.Client.PROTOCOL_COMPATIBILITY_ERR;

import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Indicates that thin client feature is not supported by the server.
 */
public class IgniteClientFeatureNotSupportedByServerException extends IgniteException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /**
     * Constructs a new exception with the specified missing feature.
     *
     * @param feature Feature.
     */
    public IgniteClientFeatureNotSupportedByServerException(String feature) {
        this("Feature " + feature + " is not supported by the server", null);
    }

    /**
     * Constructs a new exception with the specified cause and detail message.
     *
     * @param msg   the detail message.
     * @param cause the cause.
     */
    public IgniteClientFeatureNotSupportedByServerException(String msg, @Nullable Throwable cause) {
        super(PROTOCOL_COMPATIBILITY_ERR, msg, cause);
    }
}
