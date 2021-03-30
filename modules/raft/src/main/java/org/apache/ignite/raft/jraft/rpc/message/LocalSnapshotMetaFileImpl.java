package org.apache.ignite.raft.jraft.rpc.message;

import org.apache.ignite.raft.jraft.entity.LocalFileMetaOutter;
import org.apache.ignite.raft.jraft.entity.LocalStorageOutter;

class LocalSnapshotMetaFileImpl implements LocalStorageOutter.LocalSnapshotPbMeta.File, LocalStorageOutter.LocalSnapshotPbMeta.File.Builder {
    private String name;
    private LocalFileMetaOutter.LocalFileMeta meta;

    @Override public String getName() {
        return name;
    }

    @Override public LocalFileMetaOutter.LocalFileMeta getMeta() {
        return meta;
    }

    @Override public Builder setName(String name) {
        this.name = name;

        return this;
    }

    @Override public Builder setMeta(LocalFileMetaOutter.LocalFileMeta meta) {
        this.meta = meta;

        return this;
    }

    @Override public LocalStorageOutter.LocalSnapshotPbMeta.File build() {
        return this;
    }
}
