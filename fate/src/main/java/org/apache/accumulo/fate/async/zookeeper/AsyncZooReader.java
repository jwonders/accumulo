package org.apache.accumulo.fate.async.zookeeper;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.CompletionStage;

public interface AsyncZooReader {

    CompletionStage<byte[]> getData(String zPath, Stat stat);

    byte[] getData(String zPath, boolean watch, Stat stat);

    byte[] getData(String zPath, Watcher watcher, Stat stat);

    Stat getStatus(String zPath);

    Stat getStatus(String zPath, Watcher watcher);

    List<String> getChildren(String zPath);

    List<String> getChildren(String zPath, Watcher watcher);

    boolean exists(String zPath);

    boolean exists(String zPath, Watcher watcher);

    void sync(final String path);

    List<ACL> getACL(String zPath, Stat stat);

}
