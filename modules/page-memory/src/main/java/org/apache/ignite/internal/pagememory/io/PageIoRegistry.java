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

package org.apache.ignite.internal.pagememory.io;

import java.nio.ByteBuffer;
import java.util.ServiceLoader;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Page IO Registry. This component registers and provides all available {@link PageIo} types.
 */
public class PageIoRegistry {
    /**
     * Arrays of {@link IoVersions} for fast access. Element 0 is reserved.
     */
    private final IoVersions<?>[] ioVersions = new IoVersions[PageIo.MAX_IO_TYPE + 1];

    /**
     * Loads all {@link IoVersions} from a {@link PageIoModule} using the {@link ServiceLoader} mechanism.
     *
     * @return {@code this} for code chaining.
     * @throws IllegalStateException If there's an invalid page type or several {@link IoVersions} instances for the same type.
     */
    public PageIoRegistry loadAllFromServiceLoader() {
        ServiceLoader<PageIoModule> serviceLoader = ServiceLoader.load(PageIoModule.class);

        for (PageIoModule pageIoModule : serviceLoader) {
            for (IoVersions<?> ioVersions : pageIoModule.ioVersions()) {
                load(ioVersions);
            }
        }

        return this;
    }

    /**
     * Loads {@link IoVersions}'s.
     *
     * @return {@code this} for code chaining.
     * @throws IllegalStateException If there's an invalid page type or several {@link IoVersions} instances for the same type.
     */
    public PageIoRegistry load(IoVersions<?>... versions) {
        for (IoVersions<?> ioVersions : versions) {
            load(ioVersions);
        }

        return this;
    }

    private PageIoRegistry load(IoVersions<?> ioVersions) {
        if (ioVersions.getType() == 0) {
            throw new IllegalStateException("Type 0 is reserved and can't be used: " + ioVersions);
        }

        if (this.ioVersions[ioVersions.getType()] != null && !this.ioVersions[ioVersions.getType()].equals(ioVersions)) {
            throw new IllegalStateException("Duplicated IOVersions found: " + ioVersions);
        }

        this.ioVersions[ioVersions.getType()] = ioVersions;

        return this;
    }

    /**
     * Returns resolved {@link PageIo} by the {@link ByteBuffer} that contains the page.
     *
     * @param pageBuf Byte buffer with page content.
     * @throws IgniteInternalCheckedException If page type or version are invalid or not registered.
     */
    public <V extends PageIo> V resolve(ByteBuffer pageBuf) throws IgniteInternalCheckedException {
        return resolve(PageIo.getType(pageBuf), PageIo.getVersion(pageBuf));
    }

    /**
     * Returns resolved {@link PageIo} by the page address.
     *
     * @param pageAddr Memory address pointing to the page content.
     * @throws IgniteInternalCheckedException If page type or version are invalid or not registered.
     */
    public final <V extends PageIo> V resolve(long pageAddr) throws IgniteInternalCheckedException {
        return resolve(PageIo.getType(pageAddr), PageIo.getVersion(pageAddr));
    }

    /**
     * Returns resolved {@link PageIo} by the type and the version.
     *
     * @param type Page IO type.
     * @param ver Page IO version.
     * @throws IgniteInternalCheckedException If page type or version are invalid or not registered.
     */
    public <V extends PageIo> V resolve(int type, int ver) throws IgniteInternalCheckedException {
        if (type <= 0 || type > PageIo.MAX_IO_TYPE) {
            throw new IgniteInternalCheckedException("Unknown page IO type: " + type);
        }

        IoVersions<?> ios = ioVersions[type];

        if (ios == null) {
            throw new IgniteInternalCheckedException("Unknown page IO type: " + type);
        }

        return (V) ios.forVersion(ver);
    }
}
