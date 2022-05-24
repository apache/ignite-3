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

package org.apache.ignite.internal.pagememory.persistence.store;

import java.nio.ByteBuffer;

/**
 * Waiting IGNITE-17014.
 */
// TODO: IGNITE-17014 - надо дождаться
public class FilePageStore implements PageStore {
    @Override
    public void addWriteListener(PageWriteListener listener) {
    }

    @Override
    public void removeWriteListener(PageWriteListener listener) {
    }

    @Override
    public boolean exists() {
        return false;
    }

    @Override
    public long allocatePage() {
        return 0;
    }

    @Override
    public long pages() {
        return 0;
    }

    @Override
    public boolean read(long pageId, ByteBuffer pageBuf, boolean keepCrc) {
        return false;
    }

    @Override
    public void readHeader(ByteBuffer buf) {
    }

    @Override
    public void write(long pageId, ByteBuffer pageBuf, int tag, boolean calculateCrc) {
    }

    @Override
    public void sync() {
    }

    @Override
    public void ensure() {
    }

    @Override
    public int version() {
        return 0;
    }

    @Override
    public void stop(boolean clean) {
    }

    @Override
    public void truncate(int tag) {
    }

    @Override
    public int pageSize() {
        return 0;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public void close() {
    }
}
