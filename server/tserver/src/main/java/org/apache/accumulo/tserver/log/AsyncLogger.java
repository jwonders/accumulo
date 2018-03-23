package org.apache.accumulo.tserver.log;

import com.google.common.base.Joiner;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.security.crypto.CryptoModuleFactory;
import org.apache.accumulo.core.security.crypto.CryptoModuleParameters;
import org.apache.accumulo.core.security.crypto.NoFlushOutputStream;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.tserver.TabletMutations;
import org.apache.accumulo.tserver.log.DfsLogger.LogClosedException;
import org.apache.accumulo.tserver.log.DfsLogger.ServerResources;
import org.apache.accumulo.tserver.log.LogBarrier.LogFlushBarrier;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.invoke.MethodType.methodType;
import static org.apache.accumulo.tserver.log.DfsLogger.LOG_FILE_HEADER_V3;
import static org.apache.accumulo.tserver.logger.LogEvents.COMPACTION_FINISH;
import static org.apache.accumulo.tserver.logger.LogEvents.COMPACTION_START;
import static org.apache.accumulo.tserver.logger.LogEvents.DEFINE_TABLET;
import static org.apache.accumulo.tserver.logger.LogEvents.MANY_MUTATIONS;
import static org.apache.accumulo.tserver.logger.LogEvents.OPEN;

public class AsyncLogger implements Comparable<AsyncLogger> {

    private static final Logger log = LoggerFactory.getLogger(AsyncLogger.class);

    private static final DatanodeInfo[] EMPTY_PIPELINE = new DatanodeInfo[0];

    private static final LogFileValue EMPTY = new LogFileValue();
    private static final LogBarrier CLOSED_MARKER = new LogFlushBarrier();

    @GuardedBy("closeLock")
    private boolean closed = false;
    private final Object closeLock = new Object();

    private final LinkedBlockingQueue<LogBarrier> workQueue = new LinkedBlockingQueue<>();

    private final ServerResources conf;
    private FSDataOutputStream logFile;
    private DataOutputStream encryptingLogFile = null;
    private MethodHandle sync;
    private MethodHandle flush;
    private String logPath;
    //private Daemon syncThread;

    /* Track what's actually in +r/!0 for this logger ref */
    private String metaReference;
    private AtomicLong syncCounter;
    private AtomicLong flushCounter;
    private final long slowFlushMillis;

    private AsyncLogger(ServerResources conf) {
        this.conf = conf;
        this.slowFlushMillis = conf.getConfiguration().getTimeInMillis(Property.TSERV_SLOW_FLUSH_MILLIS);
    }

    public AsyncLogger(ServerResources conf, AtomicLong syncCounter, AtomicLong flushCounter) throws IOException {
        this(conf);
        this.syncCounter = syncCounter;
        this.flushCounter = flushCounter;
    }

    public String getFileName() {
        return logPath;
    }

    public Path getPath() {
        return new Path(logPath);
    }

    /**
     * Opens a Write-Ahead Log file and writes the necessary header information and OPEN entry to the file. The file is ready to be used for ingest if this method
     * returns successfully. If an exception is thrown from this method, it is the callers responsibility to ensure that {@link #close()} is called to prevent
     * leaking the file handle and/or syncing thread.
     *
     * @param address
     *          The address of the host using this WAL
     */
    public synchronized void open(String address) throws IOException {
        String filename = UUID.randomUUID().toString();
        log.debug("Address is " + address);
        String logger = Joiner.on("+").join(address.split(":"));

        log.debug("AsyncLogger.open() begin");
        VolumeManager fs = conf.getFileSystem();

        logPath = createLogPath(filename, logger, fs);

        metaReference = toString();
        LogBarrier op = null;
        try {
            logFile = createLogFile(fs);

            // Initialize the log file with a header and the crypto params used to set up this log file.
            logFile.write(LOG_FILE_HEADER_V3.getBytes(StandardCharsets.UTF_8));

            encryptingLogFile = getEncryptingLogFileStream(conf.getConfiguration());

            // In order to bootstrap the reading of this file later, we have to record the CryptoModule that was used to encipher it here,
            // so that that crypto module can re-read its own parameters.
            logFile.writeUTF(conf.getConfiguration().get(Property.CRYPTO_MODULE_CLASS));

            LogFileKey key = new LogFileKey();
            key.event = OPEN;
            key.tserverSession = filename;
            key.filename = filename;

            op = logFileData(Collections.singletonList(new Pair<>(key, EMPTY)), Durability.SYNC);

        } catch (Exception ex) {
            if (logFile != null) {
                logFile.close();
            }
            logFile = null;
            encryptingLogFile = null;
            throw new IOException(ex);
        }

        syncThread = new Daemon(new LoggingRunnable(log, new LogSyncingTask()));
        syncThread.setName("Accumulo WALog thread " + toString());
        syncThread.start();

        op.join();
        log.debug("Got new write-ahead log: " + this);
    }

    private DataOutputStream getEncryptingLogFileStream(AccumuloConfiguration config) throws IOException {
        // Initialize the crypto operations.
        String cryptoModuleClass = config.get(Property.CRYPTO_MODULE_CLASS);
        org.apache.accumulo.core.security.crypto.CryptoModule cryptoModule =
                org.apache.accumulo.core.security.crypto.CryptoModuleFactory.getCryptoModule(cryptoModuleClass);

        CryptoModuleParameters params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(config);

        NoFlushOutputStream nfos = new NoFlushOutputStream(logFile);
        params.setPlaintextOutputStream(nfos);
        params = cryptoModule.getEncryptingOutputStream(params);
        OutputStream encipheringOutputStream = params.getEncryptedOutputStream();

        // If the module just kicks back our original stream, then just use it, don't wrap it in
        // another data OutputStream.
        if (encipheringOutputStream == nfos) {
            log.debug("No enciphering, using raw output stream");
            return nfos;
        } else {
            log.debug("Enciphering found, wrapping in DataOutputStream");
            return new DataOutputStream(encipheringOutputStream);
        }
    }

    private String createLogPath(String filename, String logger, VolumeManager fs) {
        return fs.choose(Optional.empty(), ServerConstants.getBaseUris()) + Path.SEPARATOR +
                ServerConstants.WAL_DIR + Path.SEPARATOR +
                logger + Path.SEPARATOR + filename;
    }

    private FSDataOutputStream createLogFile(VolumeManager fs) throws IOException {
        return createLogFile(fs, getReplicationFactor(fs), getBlockSize());
    }

    private FSDataOutputStream createLogFile(VolumeManager fs, short replication, long blockSize) throws IOException {
        if (conf.getConfiguration().getBoolean(Property.TSERV_WAL_SYNC)) {
            return fs.createSyncable(new Path(logPath), 0, replication, blockSize);
        } else {
            return fs.create(new Path(logPath), true, 0, replication, blockSize);
        }
    }

    private long getBlockSize() {
        long blockSize = conf.getConfiguration().getAsBytes(Property.TSERV_WAL_BLOCKSIZE);
        if (blockSize == 0) {
            blockSize = (long) (conf.getConfiguration().getAsBytes(Property.TSERV_WALOG_MAX_SIZE) * 1.1);
        }
        return blockSize;
    }

    private short getReplicationFactor(VolumeManager fs) {
        short replication = (short) conf.getConfiguration().getCount(Property.TSERV_WAL_REPLICATION);
        if (replication == 0) {
            replication = fs.getDefaultReplication(new Path(logPath));
        }
        return replication;
    }

    public synchronized void defineTablet(int seq, int tid, KeyExtent tablet) throws IOException {
        // write this log to the METADATA table
        final LogFileKey key = new LogFileKey();
        key.event = DEFINE_TABLET;
        key.seq = seq;
        key.tid = tid;
        key.tablet = tablet;
        try {
            write(key, EMPTY);
        } catch (IllegalArgumentException e) {
            log.error("Signature of sync method changed. Accumulo is likely incompatible with this version of Hadoop.");
            throw new RuntimeException(e);
        }
    }

    public LogBarrier log(int seq, int tid, Mutation mutation, Durability durability) throws IOException {
        return logManyTablets(Collections.singletonList(new TabletMutations(tid, seq, Collections.singletonList(mutation), durability)));
    }

    public LogBarrier logManyTablets(List<TabletMutations> mutations) throws IOException {
        Durability durability = Durability.NONE;
        List<Pair<LogFileKey, LogFileValue>> data = new ArrayList<>();
        for (TabletMutations tabletMutations : mutations) {
            LogFileKey key = new LogFileKey();
            key.event = MANY_MUTATIONS;
            key.seq = tabletMutations.getSeq();
            key.tid = tabletMutations.getTid();
            LogFileValue value = new LogFileValue();
            value.mutations = tabletMutations.getMutations();
            data.add(new Pair<>(key, value));
            if (tabletMutations.getDurability().ordinal() > durability.ordinal()) {
                durability = tabletMutations.getDurability();
            }
        }
        return logFileData(data, durability);
    }

    public LogBarrier minorCompactionFinished(int seq, int tid, String fqfn, Durability durability) throws IOException {
        LogFileKey key = new LogFileKey();
        key.event = COMPACTION_FINISH;
        key.seq = seq;
        key.tid = tid;
        return logFileData(Collections.singletonList(new Pair<>(key, EMPTY)), durability);
    }

    public LogBarrier minorCompactionStarted(int seq, int tid, String fqfn, Durability durability) throws IOException {
        LogFileKey key = new LogFileKey();
        key.event = COMPACTION_START;
        key.seq = seq;
        key.tid = tid;
        key.filename = fqfn;
        return logFileData(Collections.singletonList(new Pair<>(key, EMPTY)), durability);
    }

    private LogBarrier logFileData(List<Pair<LogFileKey, LogFileValue>> keys, Durability durability) throws IOException {

        LogBarrier operation = LogBarrier.withDurability(durability);

        synchronized (AsyncLogger.this) {
            try {
                for (Pair<LogFileKey, LogFileValue> pair : keys) {
                    write(pair.getFirst(), pair.getSecond());
                }
            } catch (ClosedChannelException ex) {
                throw new LogClosedException();
            } catch (Exception e) {
                log.error("Failed to write log entries", e);
                operation.completeExceptionally(e);
            }
        }

        if (durability == Durability.LOG) {
            return operation;
        }

        synchronized (closeLock) {
            // use a different lock for close check so that adding to work queue does not need
            // to wait on walog I/O operations

            if (closed) {
                throw new LogClosedException();
            }
            workQueue.add(operation);
        }

        return operation;
    }

    private synchronized void write(LogFileKey key, LogFileValue value) throws IOException {
        key.write(encryptingLogFile);
        value.write(encryptingLogFile);
        encryptingLogFile.flush();
    }

    public String getLogger() {
        String parts[] = logPath.split("/");
        return Joiner.on(":").join(parts[parts.length - 2].split("[+]"));
    }

    @Override
    public int compareTo(AsyncLogger o) {
        return getFileName().compareTo(o.getFileName());
    }

  /*
   * The following method was shamelessly lifted from HBASE-11240 (sans reflection). Thanks HBase!
   */

    /**
     * This method gets the pipeline for the current walog.
     *
     * @return non-null array of DatanodeInfo
     */
    DatanodeInfo[] getPipeLine() {
        if (null != logFile) {
            OutputStream os = logFile.getWrappedStream();
            if (os instanceof DFSOutputStream) {
                return ((DFSOutputStream) os).getPipeline();
            }
        }

        // Don't have a pipeline or can't figure it out.
        return EMPTY_PIPELINE;
    }

    public void close() throws IOException {

        synchronized (closeLock) {
            if (closed)
                return;
            // after closed is set to true, nothing else should be added to the queue
            // CLOSED_MARKER should be the last thing on the queue, therefore when the
            // background thread sees the marker and exits there should be nothing else
            // to process... so nothing should be left waiting for the background
            // thread to do work
            closed = true;
            workQueue.add(CLOSED_MARKER);
        }

        // wait for background thread to finish before closing log file
        if (syncThread != null) {
            try {
                syncThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // expect workq should be empty at this point
        if (workQueue.size() != 0) {
            log.error("WAL work queue not empty after sync thread exited");
            throw new IllegalStateException("WAL work queue not empty after sync thread exited");
        }

        if (encryptingLogFile != null)
            try {
                logFile.close();
            } catch (IOException ex) {
                log.error("Failed to close log file", ex);
                throw new LogClosedException();
            }
    }

    private enum LogAwaitMethods {

        SYNC("hsync"),
        FLUSH("hflush");

        private final MethodHandle handle;

        LogAwaitMethods(String name) {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            try {
                handle = lookup.findVirtual(FSDataOutputStream.class, name, methodType(Void.class));
            } catch (NoSuchMethodException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        public void invoke() throws Throwable {
            handle.invokeExact();
        }

    }

}
