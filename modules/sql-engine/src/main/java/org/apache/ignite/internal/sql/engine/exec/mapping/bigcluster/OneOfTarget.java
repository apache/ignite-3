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

package org.apache.ignite.internal.sql.engine.exec.mapping.bigcluster;

import java.util.BitSet;
import java.util.List;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationMappingException;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTarget;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTargetFactory;

/**
 * Represents a target that should be executed on exactly one node from given list.
 *
 * <p>See javadoc of {@link ExecutionTargetFactory#oneOf(List)} for details.
 */
class OneOfTarget extends AbstractTarget {

    OneOfTarget(BitSet nodes) {
        super(nodes);
    }

    @Override
    public List<String> nodes(List<String> nodeNames) {
        int idx = nodes.nextSetBit(0);

        return List.of(nodeNames.get(idx));
    }

    @Override
    public ExecutionTarget colocateWith(ExecutionTarget other) throws ColocationMappingException {
        return ((AbstractTarget) other).colocate(this);
    }

    @Override
    public ExecutionTarget colocate(AllOfTarget other) throws ColocationMappingException {
        return colocate(other, this);
    }

    @Override
    public ExecutionTarget colocate(OneOfTarget other) throws ColocationMappingException {
        return colocate(this, other);
    }

    @Override
    public ExecutionTarget colocate(PartitionedTarget other) throws ColocationMappingException {
        return colocate(this, other);
    }

    @Override
    public ExecutionTarget colocate(SomeOfTarget other) throws ColocationMappingException {
        return colocate(this, other);
    }
}
