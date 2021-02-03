package com.alipay.sofa.jraft.rpc.message;

import com.alipay.sofa.jraft.entity.LocalStorageOutter;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.util.Marshaller;
import java.util.ArrayList;
import java.util.List;

class LocalSnapshotMetaImpl implements LocalStorageOutter.LocalSnapshotPbMeta, LocalStorageOutter.LocalSnapshotPbMeta.Builder {
    private RaftOutter.SnapshotMeta meta;
    private List<File> files = new ArrayList<>();

    @Override public RaftOutter.SnapshotMeta getMeta() {
        return meta;
    }

    @Override public List<File> getFilesList() {
        return files;
    }

    @Override public int getFilesCount() {
        return files.size();
    }

    @Override public File getFiles(int index) {
        return files.get(index);
    }

    @Override public byte[] toByteArray() {
        return Marshaller.DEFAULT.marshall(this);
    }

    @Override public boolean hasMeta() {
        return meta != null;
    }

    @Override public Builder setMeta(RaftOutter.SnapshotMeta meta) {
        this.meta = meta;

        return this;
    }

    @Override public Builder addFiles(File file) {
        files.add(file);

        return this;
    }

    @Override public LocalStorageOutter.LocalSnapshotPbMeta build() {
        return this;
    }
}
