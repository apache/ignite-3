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

package org.apache.ignite.internal;

/**
 * Contains kludges needed for the whole codebase. Should be removed as quickly as possible.
 */
public class Kludges {
    // TODO: Remove after IGNITE-20854 is fixed.
    /** Name of the property overriding idle safe time propagation period (in milliseconds). */
    public static final String IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS_PROPERTY = "IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS";
}
