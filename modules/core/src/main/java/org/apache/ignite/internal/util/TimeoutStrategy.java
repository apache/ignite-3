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
public interface TimeoutStrategy {

    /**
     * a.
     *
     * @param key a.
     * @return a.
     */
    TimeoutState getCurrent(String key);

    /**
     * a.
     *
     * @param key a.
     * @return a.
     */
    int next(String key);

    /**
     * a.
     *
     * @param key a.
     */
    void reset(String key);

    /**
     * a.
     */
    void resetAll();

    /**
     * a.
     */
    class TimeoutState {
        /**
         * a.
         */
        private final int currentTimeout;
        /**
         * a.
         */
        private final int attempt;

        /**
         * a.
         *
         * @param currentTimeout a.
         * @param attempt a.
         */
        public TimeoutState(int currentTimeout, int attempt) {
            this.currentTimeout = currentTimeout;
            this.attempt = attempt;
        }

        /**
         * a.
         */
        public int getCurrentTimeout() {
            return currentTimeout;
        }

        /**
         * a.
         */
        public int getAttempt() {
            return attempt;
        }
    }
}
