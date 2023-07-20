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

package org.apache.ignite.lang;

import java.util.UUID;

/**
 * Interface representing an exception that can be identified with trace identifier and provides an error code.
 */
public interface TraceableException {
    /**
     * Returns a unique identifier of the exception.
     *
     * @return Unique identifier of the exception.
     */
    UUID traceId();

    /**
     * Returns a full error code that includes the error's group and code, which uniquely identifies the problem within the group. This is a
     * combination of two most-significant bytes for the error group and two least-significant bytes for the error code.
     *
     * @return Full error code.
     */
    public int code();

    /**
     * Returns an error group.
     *
     * @see #code()
     * @return Error group.
     */
    public short groupCode();

    /**
     * Returns an error code that uniquely identifies the problem within a group.
     *
     * @see #code()
     * @see #groupCode()
     * @return Error code.
     */
    public short errorCode();
}
