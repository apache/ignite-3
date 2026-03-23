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

class NoOpTimeBag implements TimeBag {
    static final NoOpTimeBag INSTANCE = new NoOpTimeBag();

    @Override
    public void start() {
        // No-op.
    }

    @Override
    public void finishGlobalStage(String description) {
        // No-op.
    }

    @Override
    public void finishLocalStage(String description) {
        // No-op.
    }

    @Override
    public List<String> stagesTimings() {
        return List.of();
    }

    @Override
    public List<String> longestLocalStagesTimings(int maxPerCompositeStage) {
        return List.of();
    }
}
