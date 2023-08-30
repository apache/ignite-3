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

package org.apache.ignite.internal.systemview;

/**
 * Provides static factory methods for system view builders.
 *
 * @see ClusterSystemView
 * @see NodeSystemView
 */
public final class SystemViews {

    private SystemViews() {

    }

    /**
     * Creates an instance of a builder to construct cluster-wide system views.
     *
     * @param <T> Type of elements returned by a system view.
     * @return Returns a builder to construct cluster-wide system views.
     */
    public static <T> ClusterSystemView.Builder<T> clusterViewBuilder() {
        return new ClusterSystemView.Builder<>();
    }

    /**
     * Creates an instance of a builder to construct node system views.
     *
     * @param <T> Type of elements returned by a system view.
     * @return Returns a builder to construct node system views.
     */
    public static <T> NodeSystemView.Builder<T> nodeViewBuilder() {
        return new NodeSystemView.Builder<>();
    }
}
