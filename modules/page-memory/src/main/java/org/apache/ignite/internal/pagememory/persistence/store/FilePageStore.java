package org.apache.ignite.internal.pagememory.persistence.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.lang.IgniteInternalCheckedException;

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
    public long allocatePage() throws IgniteInternalCheckedException {
        return 0;
    }

    @Override
    public long pages() {
        return 0;
    }

    @Override
    public boolean read(long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteInternalCheckedException {
        return false;
    }

    @Override
    public void readHeader(ByteBuffer buf) throws IgniteInternalCheckedException {

    }

    @Override
    public void write(long pageId, ByteBuffer pageBuf, int tag, boolean calculateCrc) throws IgniteInternalCheckedException {

    }

    @Override
    public void sync() throws IgniteInternalCheckedException {
    }

    @Override
    public void ensure() throws IgniteInternalCheckedException {
    }

    @Override
    public int version() {
        return 0;
    }

    @Override
    public void stop(boolean clean) throws IgniteInternalCheckedException {

    }

    @Override
    public void truncate(int tag) throws IgniteInternalCheckedException {

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
    public void close() throws IOException {

    }
}
