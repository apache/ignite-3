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

package org.apache.ignite.storage.api.basic;

import org.apache.ignite.storage.api.DataRow;
import org.apache.ignite.storage.api.InvokeClosure;
import org.apache.ignite.storage.api.OperationType;
import org.jetbrains.annotations.Nullable;

public class SimpleReadInvokeClosure implements InvokeClosure {
    private DataRow row;

    @Override public void call(@Nullable DataRow row) {
        this.row = row;
    }

    @Override public DataRow newRow() {
        return null;
    }

    @Override public OperationType operationType() {
        return OperationType.NOOP;
    }

    public DataRow row() {
        return row;
    }
}
