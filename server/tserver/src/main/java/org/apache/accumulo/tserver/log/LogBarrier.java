package org.apache.accumulo.tserver.log;

import org.apache.accumulo.core.client.Durability;

import java.util.concurrent.CompletableFuture;

public abstract class LogBarrier extends CompletableFuture<Void> {

    static class LogSyncBarrier extends LogBarrier {
    }

    static class LogFlushBarrier extends LogBarrier {
    }

    static class CompletedLogBarrier extends LogBarrier {
    }

    public static LogBarrier withDurability(Durability durability) {
        switch (durability) {
            case DEFAULT:
                return new CompletedLogBarrier();
            case NONE:
                return new CompletedLogBarrier();
            case LOG:
                return new CompletedLogBarrier();
            case FLUSH:
                return new LogFlushBarrier();
            case SYNC:
                return new LogSyncBarrier();
            default:
                throw new AssertionError("Impossible case.");
        }
    }

}
