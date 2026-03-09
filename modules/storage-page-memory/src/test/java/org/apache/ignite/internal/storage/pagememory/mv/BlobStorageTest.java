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

package org.apache.ignite.internal.storage.pagememory.mv;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.storage.pagememory.mv.MvPageTypes.T_BLOB_FRAGMENT_IO;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.configuration.VolatileDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.inmemory.VolatilePageMemory;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.reuse.ReuseBag;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.storage.pagememory.mv.io.BlobFragmentIo;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(ConfigurationExtension.class)
@ExtendWith(MockitoExtension.class)
class BlobStorageTest extends BaseIgniteAbstractTest {
    private static final int PAGE_SIZE = 1024;

    @Mock
    private ReuseList reuseList;

    private PageMemory pageMemory;

    @Captor
    private ArgumentCaptor<ReuseBag> reuseBagCaptor;

    private BlobStorage blobStorage;

    @BeforeEach
    void createStorage() {
        PageIoRegistry pageIoRegistry = new PageIoRegistry() {
            {
                ioVersions[T_BLOB_FRAGMENT_IO] = BlobFragmentIo.VERSIONS;
            }
        };

        pageMemory = spy(new VolatilePageMemory(
                VolatileDataRegionConfiguration.builder().pageSize(PAGE_SIZE).initSize(256000000).maxSize(256000000).build(),
                pageIoRegistry,
                new OffheapReadWriteLock(OffheapReadWriteLock.DEFAULT_CONCURRENCY_LEVEL)
        ));

        blobStorage = new BlobStorage(reuseList, pageMemory, 1, 1);
    }

    @Test
    void attemptsReuseOnAddition() throws Exception {
        blobStorage.addBlob("test".getBytes(UTF_8));

        verify(reuseList).takeRecycledPage();
    }

    @Test
    void allocatesOnlyRequiredNumberOfPages() throws Exception {
        blobStorage.addBlob(new byte[(int) (PAGE_SIZE * 1.5)]);

        verify(pageMemory, times(2)).allocatePageNoReuse(anyInt(), anyInt(), anyByte());
    }

    @Test
    void allocatesOnlyRequiredNumberOfPagesOnUpdate() throws Exception {
        long pageId = blobStorage.addBlob(new byte[(int) (PAGE_SIZE * 1.5)]);

        blobStorage.updateBlob(pageId, new byte[(int) (PAGE_SIZE * 2.5)]);

        verify(pageMemory, times(3)).allocatePageNoReuse(anyInt(), anyInt(), anyByte());
    }

    @Test
    void doesNotAllocateOnShortening() throws Exception {
        long pageId = blobStorage.addBlob(new byte[(int) (PAGE_SIZE * 1.5)]);

        blobStorage.updateBlob(pageId, new byte[(int) (PAGE_SIZE * 0.5)]);

        verify(pageMemory, times(2)).allocatePageNoReuse(anyInt(), anyInt(), anyByte());
    }

    @Test
    void returnsAddedBlob() throws Exception {
        byte[] payload = "test".getBytes(UTF_8);

        long pageId = blobStorage.addBlob(payload);

        assertThat(blobStorage.readBlob(pageId), is(equalTo(payload)));
    }

    @Test
    void updatesExistingBlob() throws Exception {
        long pageId = blobStorage.addBlob("test".getBytes(UTF_8));

        blobStorage.updateBlob(pageId, "Hello".getBytes(UTF_8));

        assertThat(blobStorage.readBlob(pageId), is("Hello".getBytes(UTF_8)));
    }

    @Test
    void updatesAddingNewPages() throws Exception {
        long pageId = blobStorage.addBlob("test".getBytes(UTF_8));

        byte[] newBytes = "Hello".repeat(1000).getBytes(UTF_8);
        blobStorage.updateBlob(pageId, newBytes);

        assertThat(blobStorage.readBlob(pageId), is(equalTo(newBytes)));
    }

    @Test
    void updatesToLessPages() throws Exception {
        long pageId = blobStorage.addBlob(new byte[PAGE_SIZE * 2]);

        blobStorage.updateBlob(pageId, new byte[0]);

        assertThat(blobStorage.readBlob(pageId), is(new byte[0]));
    }

    @Test
    void freedPagesAreRecycled() throws Exception {
        long pageId = blobStorage.addBlob(new byte[PAGE_SIZE * 2]);

        blobStorage.updateBlob(pageId, new byte[0]);

        verify(pageMemory, times(3)).allocatePageNoReuse(anyInt(), anyInt(), anyByte());

        verify(reuseList).addForRecycle(reuseBagCaptor.capture());

        ReuseBag capturedBag = reuseBagCaptor.getValue();

        assertThat(collectBagPages(capturedBag), hasSize(2));
    }

    private static List<Long> collectBagPages(ReuseBag capturedBag) {
        List<Long> pageIds = new ArrayList<>();

        while (!capturedBag.isEmpty()) {
            pageIds.add(capturedBag.pollFreePage());
        }

        return pageIds;
    }
}
