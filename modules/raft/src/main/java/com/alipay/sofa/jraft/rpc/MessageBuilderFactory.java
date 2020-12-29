package com.alipay.sofa.jraft.rpc;

import com.alipay.sofa.jraft.rpc.message.DefaultMessageBuilderFactory;

public interface MessageBuilderFactory {
    public static MessageBuilderFactory DEFAULT = new DefaultMessageBuilderFactory();

    CliRequests.AddPeerRequest.Builder create();
}
