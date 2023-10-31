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

package org.apache.ignite.raft.jraft.storage.logit.storage.factory;

import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.logit.option.StoreOptions;
import org.apache.ignite.raft.jraft.storage.logit.storage.db.AbstractDB;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.AbstractFile;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.FileManager;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.FileType;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.index.IndexFile;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.segment.SegmentFile;
import org.apache.ignite.raft.jraft.storage.logit.storage.service.AllocateFileService;
import org.apache.ignite.raft.jraft.storage.logit.storage.service.ServiceManager;

/**
 * The factory that provides uniform construction functions
 */
public class LogStoreFactory {
    private final StoreOptions storeOptions;
    private final RaftOptions raftOptions;

    public LogStoreFactory(final StoreOptions opts, RaftOptions raftOptions) {
        this.storeOptions = opts;
        this.raftOptions = raftOptions;
    }

    /**
     * Create new file(index/segment/conf)
     */
    public AbstractFile newFile(final FileType fileType, final String filePath) {
        switch (fileType) {
            case FILE_INDEX:
                return new IndexFile(raftOptions, filePath, this.storeOptions.getIndexFileSize());

            case FILE_SEGMENT:
                return new SegmentFile(raftOptions, filePath, this.storeOptions.getSegmentFileSize());

            case FILE_CONFIGURATION:
                return new SegmentFile(raftOptions, filePath, this.storeOptions.getConfFileSize());

            default:
                throw new AssertionError("Unidentified file type: " + fileType);
        }
    }

    /**
     * Create new fileManager(index/segment/conf)
     */
    public FileManager newFileManager(final FileType fileType, final String storePath,
                                      final AllocateFileService allocateService) {
        final FileManager.FileManagerBuilder fileManagerBuilder = FileManager.newBuilder() //
            .fileType(fileType) //
            .fileSize(getFileSize(fileType)) //
            .storePath(storePath) //
            .logStoreFactory(this) //
            .allocateService(allocateService);
        return fileManagerBuilder.build();
    }

    /**
     * Create new serviceManager
     */
    public ServiceManager newServiceManager(final AbstractDB abstractDB) {
        return new ServiceManager(abstractDB);
    }

    /**
     * Create new allocateFileService
     */
    public AllocateFileService newAllocateService(final AbstractDB abstractDB) {
        return new AllocateFileService(abstractDB, this);
    }

    public int getFileSize(final FileType fileType) {
        return isIndex(fileType) ? this.storeOptions.getIndexFileSize() : //
            isConf(fileType) ? this.storeOptions.getConfFileSize() : //
                this.storeOptions.getSegmentFileSize();
    }

    private boolean isIndex(final FileType fileType) {
        return fileType == FileType.FILE_INDEX;
    }

    private boolean isConf(final FileType fileType) {
        return fileType == FileType.FILE_CONFIGURATION;
    }

    public StoreOptions getStoreOptions() {
        return this.storeOptions;
    }

    public RaftOptions getRaftOptions() {
        return raftOptions;
    }
}
