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

package org.apache.ignite.internal.metastorage.common.command;

public class CompoundConditionInfo implements ConditionInfo {

    private final ConditionInfo leftConditionInfo;
    private final ConditionInfo rightConditionInfo;
    private final CompoundConditionType type;

    public CompoundConditionInfo(ConditionInfo leftConditionInfo,
            ConditionInfo rightConditionInfo, CompoundConditionType type) {
        this.leftConditionInfo = leftConditionInfo;
        this.rightConditionInfo = rightConditionInfo;
        this.type = type;
    }

    public ConditionInfo leftConditionInfo() {
        return leftConditionInfo;
    }

    public ConditionInfo rightConditionInfo() {
        return rightConditionInfo;
    }

    public CompoundConditionType type() {
        return type;
    }

}
