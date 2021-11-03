/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMapping;

/**
 * Regular query or DML
 */
public interface MultiStepPlan extends QueryPlan {
    /**
     * @return Query fragments.
     */
    List<Fragment> fragments();

    /**
     * @return Fields metadata.
     */
    ResultSetMetadataInternal metadata();

    /**
     * @param fragment Fragment.
     * @return Mapping for a given fragment.
     */
    FragmentMapping mapping(Fragment fragment);

    /**
     *
     */
    ColocationGroup target(Fragment fragment);

    /**
     *
     */
    Map<Long, List<String>> remotes(Fragment fragment);

    /**
     * Inits query fragments.
     *
     * @param ctx Planner context.
     */
    void init(PlanningContext ctx);
}
