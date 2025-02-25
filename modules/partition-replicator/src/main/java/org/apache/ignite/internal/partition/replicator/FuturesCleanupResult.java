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

package org.apache.ignite.internal.partition.replicator;

/**
 * Contains result of cleanup futures await.
 */
public class FuturesCleanupResult {
    private final boolean hadReadFutures;
    private final boolean hadUpdateFutures;
    private final boolean forceCleanup;

    /** Constructor. */
    public FuturesCleanupResult(boolean hadReadFutures, boolean hadUpdateFutures, boolean forceCleanup) {
        this.hadReadFutures = hadReadFutures;
        this.hadUpdateFutures = hadUpdateFutures;
        this.forceCleanup = forceCleanup;
    }

    public boolean hadReadFutures() {
        return hadReadFutures;
    }

    public boolean hadUpdateFutures() {
        return hadUpdateFutures;
    }

    public boolean forceCleanup() {
        return forceCleanup;
    }

    public boolean shouldApplyWriteIntent() {
        return hadUpdateFutures() || forceCleanup();
    }
}
