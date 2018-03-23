package org.apache.accumulo.fate.async;

import org.apache.zookeeper.AsyncCallback.Create2Callback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class AsyncZooKeeper {

    private final ZooKeeper zookeeper;

    public AsyncZooKeeper(ZooKeeper zookeeper) {
        this.zookeeper = zookeeper;
    }

    public CompletionStage<Void> create(String path, byte[] data, List<ACL> acl, CreateMode createMode) {
        Create2Callback callback = new Create2Callback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name, Stat stat) {

            }
        };
        zookeeper.create(path, data, acl, createMode, callback, null);
        return CompletableFuture.completedFuture(null);
    }

    public CompletionStage<Void> createWithTtl() {
        return CompletableFuture.completedFuture(null);
    }

    public CompletionStage<Void> createAndGetName() {
        return CompletableFuture.completedFuture(null);
    }



}
