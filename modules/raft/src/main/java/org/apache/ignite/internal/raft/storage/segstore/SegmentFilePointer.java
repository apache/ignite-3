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

package org.apache.ignite.internal.raft.storage.segstore;

import org.apache.ignite.internal.tostring.S;

class SegmentFilePointer {
    private final FileProperties fileProperties;

    private final int payloadOffset;

    SegmentFilePointer(FileProperties fileProperties, int payloadOffset) {
        this.fileProperties = fileProperties;
        this.payloadOffset = payloadOffset;
    }

    FileProperties fileProperties() {
        return fileProperties;
    }

    int payloadOffset() {
        return payloadOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SegmentFilePointer that = (SegmentFilePointer) o;
        return payloadOffset == that.payloadOffset && fileProperties.equals(that.fileProperties);
    }

    @Override
    public int hashCode() {
        int result = fileProperties.hashCode();
        result = 31 * result + payloadOffset;
        return result;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
