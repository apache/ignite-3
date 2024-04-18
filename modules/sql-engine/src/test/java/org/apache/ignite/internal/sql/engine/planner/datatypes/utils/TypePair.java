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

package org.apache.ignite.internal.sql.engine.planner.datatypes.utils;

import org.apache.ignite.internal.type.NativeType;

/**
 * Utility interface that describes pair of types.
 *
 * <p>Generally used in tests verifying behaviour of binary operations.
 *
 * <p>Although elements of pair are called "{@link #first()}" and {@link #second()}, whether
 * the order does really matter or not is defined by particular test.
 */
public interface TypePair {
    /** Returns the first type of the pair. */
    NativeType first();

    /** Returns the second type of the pair. */
    NativeType second();
}
