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

package org.apache.ignite.internal.pagememory.persistence.throttling;

import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;

/**
 * Factory interface to create instances of {@link PagesWriteThrottlePolicy}.
 */
@FunctionalInterface
public interface ThrottlingPolicyFactory {
    /**
     * Creates an instance of a throttler. The cyclic dependency between throttlers and page memory is intentional here. Different
     * implementations require an access to different parts of the data region, which would make the generalization of all these parts
     * really awkward in this interface. We chose the lesser evil.
     *
     * @param pageMemory A page memory instance to throttle.
     */
    PagesWriteThrottlePolicy createThrottlingPolicy(PersistentPageMemory pageMemory);
}
