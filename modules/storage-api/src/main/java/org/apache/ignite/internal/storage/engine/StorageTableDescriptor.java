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

package org.apache.ignite.internal.storage.engine;

/**
 * Table descriptor.
 */
public class StorageTableDescriptor {
    private final int id;

    private final int partitions;

    private final String dataRegion;

    /**
     * Constructor.
     *
     * @param id Table ID.
     * @param partitions Count of partitions.
     * @param dataRegion Data region name.
     */
    public StorageTableDescriptor(int id, int partitions, String dataRegion) {
        this.id = id;
        this.partitions = partitions;
        this.dataRegion = dataRegion;
    }

    /**
     * Returns the table ID.
     */
    public int getId() {
        return id;
    }

    /**
     * Returns count of partitions.
     */
    public int getPartitions() {
        return partitions;
    }

    /**
     * Returns the data region name.
     */
    public String getDataRegion() {
        return dataRegion;
    }
}
