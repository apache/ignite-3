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

package org.apache.ignite.internal.metastorage.timebag;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Utility class to measure and collect timings of some execution workflow.
 */
public interface TimeBag {
    /**
     * Starts this time bag.
     */
    void start();

    /**
     * Finishes the global stage with given description and resets the local stages.
     *
     * @param description Description.
     */
    void finishGlobalStage(String description);

    /**
     * Finishes the local stage with given description.
     *
     * @param description Description.
     */
    void finishLocalStage(String description);

    /**
     * Returns list of string representation of all stage timings.
     *
     * @return List of string representation of all stage timings.
     */
    List<String> stagesTimings();

    /**
     * Returns list of string representation of longest local stages per each composite stage.
     *
     * @param maxPerCompositeStage Max count of local stages to collect per composite stage.
     * @return List of string representation of longest local stages per each composite stage.
     */
    List<String> longestLocalStagesTimings(int maxPerCompositeStage);

    /**
     * Creates a new instance of {@link TimeBag}.
     *
     * @param enabled Flag indicating whether the time bag is enabled or not.
     * @param started Flag indicating whether the time bag is started or not.
     * @return New instance of {@link TimeBag}.
     */
    static TimeBag createTimeBag(boolean enabled, boolean started) {
        return createTimeBag(enabled, started, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a new instance of {@link TimeBag}.
     *
     * @param enabled Flag indicating whether the time bag is enabled or not.
     * @param started Flag indicating whether the time bag is started or not.
     * @return New instance of {@link TimeBag}.
     */
    static TimeBag createTimeBag(boolean enabled, boolean started, TimeUnit measurementUnit) {
        return enabled ? new TimeBagImpl(started, measurementUnit) : NoOpTimeBag.INSTANCE;
    }
}
