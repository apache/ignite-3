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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import java.util.List;

/**
 * Intermediate result returned by {@link FragmentMapper}.
 *
 * <p>In general, fragments have exactly one colocation group. But in case of MAP phase of 2-phase
 * SET operator fragment may be mapped to an arbitrary number of colocation groups. MAP phase does
 * pre-aggregation, thus we don't care about colocation of its inputs. Final result calculation will
 * be made on a reducer, which must be colocated.
 *
 * <p>That's why we need additional container here.
 */
class FragmentMapping {
    private final List<ColocationGroup> groups;

    FragmentMapping(List<ColocationGroup> groups) {
        this.groups = groups;
    }

    public List<ColocationGroup> groups() {
        return groups;
    }
}
