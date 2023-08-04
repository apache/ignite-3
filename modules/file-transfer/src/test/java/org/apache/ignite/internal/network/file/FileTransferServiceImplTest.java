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

package org.apache.ignite.internal.network.file;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import java.nio.file.Path;
import org.apache.ignite.internal.network.file.messages.FileTransferMessageType;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessageHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(WorkDirectoryExtension.class)
class FileTransferServiceImplTest {

    @WorkDirectory
    private Path workDir;

    @Mock
    private FileSender fileSender;

    @Mock
    private FileReceiver fileReceiver;

    @Spy
    private TestMessagingService messagingService = new TestMessagingService();

    @Spy
    private TestTopologyService topologyService = new TestTopologyService();

    @Test
    void fileTransfersCanceledWhenSenderLeft() {
        FileTransferServiceImpl fileTransferService = new FileTransferServiceImpl(
                topologyService,
                messagingService,
                workDir,
                fileSender,
                fileReceiver
        );

        fileTransferService.start();
        verify(messagingService).addMessageHandler(eq(FileTransferMessageType.class), any(NetworkMessageHandler.class));
        verify(topologyService).addEventHandler(any());

        topologyService.fairDisappearedEvent(new ClusterNode("node1", "localhost", new NetworkAddress("localhost", 1234)));

        verify(fileReceiver).cancelTransfersFromSender("node1");
    }
}
