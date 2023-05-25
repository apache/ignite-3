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

package org.apache.ignite.table;

// TODO: Builder pattern
public class DataStreamerOptions {
    private int batchSize = 1000;

    private int perNodeParallelOperations = 4;

    public int batchSize() {
        return batchSize;
    }

    public DataStreamerOptions batchSize(int batchSize) {
        this.batchSize = batchSize;

        return this;
    }

    public int perNodeParallelOperations() {
        return perNodeParallelOperations;
    }

    public void perNodeParallelOperations(int perNodeParallelOperations) {
        this.perNodeParallelOperations = perNodeParallelOperations;
    }
}
