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

package org.apache.ignite.internal.util;

/**
 * a.
 */
public class NoopTimeoutStrategy implements TimeoutStrategy {
    /**
     * a.
     */
    private final TimeoutState timeoutState;

    /**
     * a.
     *
     * @param initTimeout a.
     */
    public NoopTimeoutStrategy(int initTimeout) {
        this.timeoutState = new TimeoutState(initTimeout, 0);
    }

    /** {@inheritDoc} */
    @Override
    public TimeoutState getCurrent(String key) {
        return timeoutState;
    }

    /** {@inheritDoc} */
    @Override
    public int next(String key) {
        return timeoutState.getCurrentTimeout();
    }

    /** {@inheritDoc} */
    @Override
    public void reset(String key) {}

    /** {@inheritDoc} */
    @Override
    public void resetAll() {}
}
