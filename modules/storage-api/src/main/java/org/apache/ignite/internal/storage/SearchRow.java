/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage;

import java.nio.ByteBuffer;

/**
 * Interface to be used as a key representation to search data in storage.
 */
public interface SearchRow {
    /**
     * Returns key bytes.
     */
    byte[] keyBytes();

    /**
     * Returns key object as a byte buffer. Allows more effective memory management in certain cases. Position of the resulting buffer
     * must be {@code 0}.
     */
    ByteBuffer key();
}
