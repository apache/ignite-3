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

package org.apache.ignite.raft.jraft.storage.logit.storage.file;

/**
 * Specifies the type of file
 */
public enum FileType {
    FILE_INDEX("indexFile", ".i"), // index file
    FILE_SEGMENT("segmentFile", ".s"), // segment file
    FILE_CONFIGURATION("confFile", ".c"); // configuration file

    private final String fileName;
    private final String fileSuffix;

    FileType(String fileName, String fileSuffix) {
        this.fileName = fileName;
        this.fileSuffix = fileSuffix;
    }

    public String getFileName() {
        return fileName;
    }

    public String getFileSuffix() {
        return fileSuffix;
    }
}
