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

package org.apache.ignite.internal.fileio;

/**
 * Metrics for file I/O operations.
 */
public interface FileIoMetrics {
    /**
     * Records a read operation.
     *
     * @param bytesRead Number of bytes read (positive on success, 0 for EOF, -1 on error).
     * @param durationNanos Operation duration in nanoseconds.
     */
    void recordRead(int bytesRead, long durationNanos);

    /**
     * Records a write operation.
     *
     * @param bytesWritten Number of bytes written (positive on success, 0 or -1 on error).
     * @param durationNanos Operation duration in nanoseconds.
     */
    void recordWrite(int bytesWritten, long durationNanos);
}
