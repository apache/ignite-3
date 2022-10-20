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

package org.apache.ignite.internal.lock;

/**
 * Represents a lockup (this is an aquisition and owning of a lock like {@link java.util.concurrent.locks.Lock})
 * that plays nicely with try-with-resources clause. The underlying lock will be acquired on creation (or obtaining)
 * a lockup, and the lock will be released when {@link AutoLockup#close()} method is called.
 */
public interface AutoLockup extends AutoCloseable {
    /**
     * Releases the underlying lock.
     * Does not declare any thrown exception to facilitate usage in try-with-resources clauses.
     */
    @Override
    void close();
}
