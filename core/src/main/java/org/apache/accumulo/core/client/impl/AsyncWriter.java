package org.apache.accumulo.core.client.impl;

import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;

public class AsyncWriter {

    public static void main(String[] args) throws Exception {
        TNonblockingTransport transport = new TNonblockingSocket("127.0.0.1", 9160);
        TAsyncClientManager clientManager = new TAsyncClientManager();
        TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
        TabletClientService.AsyncClient client = new TabletClientService.AsyncClient(protocolFactory, clientManager, transport);


        client.conditionalUpdate();
    }

}
