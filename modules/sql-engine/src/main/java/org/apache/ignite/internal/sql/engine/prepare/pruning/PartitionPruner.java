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

package org.apache.ignite.internal.sql.engine.prepare.pruning;

import java.util.List;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappedFragment;

/** Applies partition pruning. */
@FunctionalInterface
public interface PartitionPruner {

    /**
     * Applies partition pruning to the given fragments.
     *
     * @param mappedFragments List of fragments.
     * @param dynamicParameters Dynamic parameter values.
     *
     * @return New list of mapped fragments, if partition pruning was applied. Otherwise returns `mappedFragments`.
     */
    List<MappedFragment> apply(List<MappedFragment> mappedFragments, Object[] dynamicParameters);
}
