package org.apache.ignite.raft.jraft.rpc.message;

import org.apache.ignite.raft.jraft.entity.LocalFileMetaOutter;
import org.apache.ignite.raft.jraft.rpc.Message;

// TODO asch user meta.
public class LocalFileMetaImpl implements LocalFileMetaOutter.LocalFileMeta, LocalFileMetaOutter.LocalFileMeta.Builder {
    //private ByteString userMeta; // TODO asch not used currently.
    private LocalFileMetaOutter.FileSource fileSource;
    private String checksum;

    //@Override public ByteString getUserMeta() {
//        return userMeta;
//    }

    @Override public LocalFileMetaOutter.FileSource getSource() {
        return fileSource;
    }

    @Override public String getChecksum() {
        return checksum;
    }

    @Override public boolean hasChecksum() {
        return checksum != null;
    }

    //@Override public boolean hasUserMeta() {
//        return userMeta != null;
//    }

    @Override public boolean hasUserMeta() {
        return false;
    }

    @Override public LocalFileMetaOutter.LocalFileMeta build() {
        return this;
    }

//    @Override public Builder setUserMeta(ByteString data) {
//        this.userMeta = data;
//
//        return this;
//    }

    @Override public void mergeFrom(Message fileMeta) {
        LocalFileMetaOutter.LocalFileMeta tmp = (LocalFileMetaOutter.LocalFileMeta) fileMeta;

        //this.userMeta = tmp.getUserMeta();
        this.fileSource = tmp.getSource();
        this.checksum = tmp.getChecksum();
    }

    @Override public Builder setChecksum(String checksum) {
        this.checksum = checksum;

        return this;
    }

    @Override public Builder setSource(LocalFileMetaOutter.FileSource source) {
        this.fileSource = source;

        return this;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocalFileMetaImpl that = (LocalFileMetaImpl) o;

        if (fileSource != that.fileSource) return false;
        return checksum.equals(that.checksum);
    }

    @Override public int hashCode() {
        int result = fileSource.hashCode();
        result = 31 * result + checksum.hashCode();
        return result;
    }
}
