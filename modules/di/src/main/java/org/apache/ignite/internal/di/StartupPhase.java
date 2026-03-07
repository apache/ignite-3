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

package org.apache.ignite.internal.di;

/**
 * Defines the startup phase in which an {@link org.apache.ignite.internal.manager.IgniteComponent} should be started.
 *
 * <p>Ignite node startup happens in two phases:
 * <ul>
 *     <li>{@link #PHASE_1} — before the node joins the cluster (pre-join).</li>
 *     <li>{@link #PHASE_2} — after the node joins the cluster (post-join).</li>
 * </ul>
 */
public enum StartupPhase {
    /** Components started before the node joins the cluster. */
    PHASE_1,

    /** Components started after the node joins the cluster. */
    PHASE_2
}
