package com.alipay.sofa.jraft.rpc.message;

import com.alipay.sofa.jraft.entity.LocalFileMetaOutter;
import com.alipay.sofa.jraft.rpc.CliRequests;
import com.alipay.sofa.jraft.rpc.MessageBuilderFactory;

public class DefaultMessageBuilderFactory implements MessageBuilderFactory {
    @Override public CliRequests.AddPeerRequest.Builder createAddPeer() {
        return new AddPeerRequestImpl();
    }

    @Override public LocalFileMetaOutter.LocalFileMeta.Builder createLocalFileMeta() {
        return new LocalFileMetaImpl();
    }
}
