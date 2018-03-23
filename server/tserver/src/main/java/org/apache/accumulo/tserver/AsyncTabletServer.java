package org.apache.accumulo.tserver;

import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.DurabilityImpl;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletType;
import org.apache.accumulo.core.client.impl.thrift.ConfigurationType;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.data.thrift.MapFileInfo;
import org.apache.accumulo.core.data.thrift.TColumn;
import org.apache.accumulo.core.data.thrift.TConditionalMutation;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.data.thrift.TMutation;
import org.apache.accumulo.core.data.thrift.TRange;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TDurability;
import org.apache.accumulo.core.tabletserver.thrift.TSamplerConfiguration;
import org.apache.accumulo.core.tabletserver.thrift.TUnloadTabletGoal;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.data.ServerMutation;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.tserver.log.TabletServerLogger;
import org.apache.accumulo.tserver.tablet.CommitSession;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static java.nio.charset.StandardCharsets.UTF_8;

public class AsyncTabletServer extends AccumuloServerContext implements TabletClientService.AsyncIface {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncTabletServer.class);

    private /*final*/ TabletServerLogger logger;
    private /*final*/ SecurityOperation security;
    private /*final*/ TabletServerResourceManager resourceManager;
    private /*final*/ SortedMap<KeyExtent,Tablet> onlineTablets = Collections.synchronizedSortedMap(new TreeMap<KeyExtent,Tablet>());

    private final Map<TabletType, CompletionBarrier> writeTrackers = new EnumMap<>(TabletType.class);

    public AsyncTabletServer(ServerConfigurationFactory confFactory, TabletServerLogger logger, SecurityOperation security, TabletServerResourceManager resourceManager, SortedMap<KeyExtent, Tablet> onlineTablets) {
        super(confFactory);
        this.logger = logger;
        this.security = security;
        this.resourceManager = resourceManager;
        this.onlineTablets = onlineTablets;
    }

    @Override
    public void startScan(TInfo tinfo, TCredentials credentials, TKeyExtent extent, TRange range, List<TColumn> columns, int batchSize, List<IterInfo> ssiList, Map<String, Map<String, String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites, boolean isolated, long readaheadThreshold, TSamplerConfiguration samplerConfig, long batchTimeOut, String classLoaderContext, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void continueScan(TInfo tinfo, long scanID, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void closeScan(TInfo tinfo, long scanID, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void startMultiScan(TInfo tinfo, TCredentials credentials, Map<TKeyExtent, List<TRange>> batch, List<TColumn> columns, List<IterInfo> ssiList, Map<String, Map<String, String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites, TSamplerConfiguration samplerConfig, long batchTimeOut, String classLoaderContext, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void continueMultiScan(TInfo tinfo, long scanID, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void closeMultiScan(TInfo tinfo, long scanID, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void startUpdate(TInfo tinfo, TCredentials credentials, TDurability durability, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void applyUpdates(TInfo tinfo, long updateID, TKeyExtent keyExtent, List<TMutation> mutations, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void closeUpdate(TInfo tinfo, long updateID, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void update(TInfo tinfo, TCredentials credentials, TKeyExtent tkeyExtent, TMutation tmutation, TDurability tdurability, AsyncMethodCallback resultHandler) throws TException {

        final String tableId = new String(tkeyExtent.getTable(), UTF_8);

        String namespaceId = getNamespaceId(credentials, tableId);

        if (!security.canWrite(credentials, tableId, namespaceId)) {
            throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }

        final KeyExtent keyExtent = new KeyExtent(tkeyExtent);
        final Tablet tablet = onlineTablets.get(new KeyExtent(keyExtent));
        if (tablet == null) {
            throw new NotServingTabletException(tkeyExtent);
        }

        if (!keyExtent.isMeta()) {
            try {
                AsyncTabletServer.this.resourceManager.waitUntilCommitsAreEnabled();
            } catch (HoldTimeoutException e) {
                // Major hack. Assumption is that the client has timed out and is gone.
                // If thats not the case, then throwing the following will let client know there
                // was a failure and it should retry.
                throw new NotServingTabletException(tkeyExtent);
            }
        }

        final Mutation mutation = new ServerMutation(tmutation);
        final List<Mutation> mutations = Collections.singletonList(mutation);

        CompletableFuture<Void> writeOperation = CompletableFuture.supplyAsync(() -> {
            try {
                CommitSession cs = tablet.prepareMutationsForCommit(new TservConstraintEnv(security, credentials), mutations);
                if (cs == null) {
                    throw new CompletionException(new NotServingTabletException(tkeyExtent));
                }
                return cs;
            } catch (TConstraintViolationException e) {
                throw new CompletionException(e);
            }
        }).thenApply(cs -> {
            try {
                final Durability durability = DurabilityImpl.resolveDurabilty(DurabilityImpl.fromThrift(tdurability), tablet.getDurability());
                logger.log(cs, cs.getWALogSeq(), mutation, durability);
            } catch (IOException e) {
                LOG.warn("Error writing mutations to log", e);
            }
            return cs;
        }).thenAccept(cs -> cs.commit(mutations));

        CompletionBarrier barrier = writeTrackers.get(TabletType.type(keyExtent));
        barrier.submit(writeOperation);
    }

    private String getNamespaceId(TCredentials credentials, String tableId) throws ThriftSecurityException {
        try {
            return Tables.getNamespaceId(getInstance(), tableId);
        } catch (TableNotFoundException e) {
            throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.TABLE_DOESNT_EXIST);
        }
    }

    @Override
    public void startConditionalUpdate(TInfo tinfo, TCredentials credentials, List<ByteBuffer> authorizations, String tableID, TDurability durability, String classLoaderContext, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void conditionalUpdate(TInfo tinfo, long sessID, Map<TKeyExtent, List<TConditionalMutation>> mutations, List<String> symbols, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void invalidateConditionalUpdate(TInfo tinfo, long sessID, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void closeConditionalUpdate(TInfo tinfo, long sessID, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void bulkImport(TInfo tinfo, TCredentials credentials, long tid, Map<TKeyExtent, Map<String, MapFileInfo>> files, boolean setTime, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void splitTablet(TInfo tinfo, TCredentials credentials, TKeyExtent extent, ByteBuffer splitPoint, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void loadTablet(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent extent, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void unloadTablet(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent extent, TUnloadTabletGoal goal, long requestTime, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void flush(TInfo tinfo, TCredentials credentials, String lock, String tableId, ByteBuffer startRow, ByteBuffer endRow, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void flushTablet(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent extent, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void chop(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent extent, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void compact(TInfo tinfo, TCredentials credentials, String lock, String tableId, ByteBuffer startRow, ByteBuffer endRow, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void getTabletServerStatus(TInfo tinfo, TCredentials credentials, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void getTabletStats(TInfo tinfo, TCredentials credentials, String tableId, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void getHistoricalStats(TInfo tinfo, TCredentials credentials, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void halt(TInfo tinfo, TCredentials credentials, String lock, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void fastHalt(TInfo tinfo, TCredentials credentials, String lock, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void getActiveScans(TInfo tinfo, TCredentials credentials, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void getActiveCompactions(TInfo tinfo, TCredentials credentials, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void removeLogs(TInfo tinfo, TCredentials credentials, List<String> filenames, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void getActiveLogs(TInfo tinfo, TCredentials credentials, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void getRootTabletLocation(AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void getInstanceId(AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void getZooKeepers(AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void bulkImportFiles(TInfo tinfo, TCredentials credentials, long tid, String tableId, List<String> files, String errorDir, boolean setTime, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void isActive(TInfo tinfo, long tid, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void ping(TCredentials credentials, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void getDiskUsage(Set<String> tables, TCredentials credentials, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void listLocalUsers(TInfo tinfo, TCredentials credentials, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void createLocalUser(TInfo tinfo, TCredentials credentials, String principal, ByteBuffer password, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void dropLocalUser(TInfo tinfo, TCredentials credentials, String principal, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void changeLocalUserPassword(TInfo tinfo, TCredentials credentials, String principal, ByteBuffer password, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void authenticate(TInfo tinfo, TCredentials credentials, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void authenticateUser(TInfo tinfo, TCredentials credentials, TCredentials toAuth, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void changeAuthorizations(TInfo tinfo, TCredentials credentials, String principal, List<ByteBuffer> authorizations, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void getUserAuthorizations(TInfo tinfo, TCredentials credentials, String principal, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void hasSystemPermission(TInfo tinfo, TCredentials credentials, String principal, byte sysPerm, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void hasTablePermission(TInfo tinfo, TCredentials credentials, String principal, String tableName, byte tblPerm, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void hasNamespacePermission(TInfo tinfo, TCredentials credentials, String principal, String ns, byte tblNspcPerm, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void grantSystemPermission(TInfo tinfo, TCredentials credentials, String principal, byte permission, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void revokeSystemPermission(TInfo tinfo, TCredentials credentials, String principal, byte permission, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void grantTablePermission(TInfo tinfo, TCredentials credentials, String principal, String tableName, byte permission, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void revokeTablePermission(TInfo tinfo, TCredentials credentials, String principal, String tableName, byte permission, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void grantNamespacePermission(TInfo tinfo, TCredentials credentials, String principal, String ns, byte permission, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void revokeNamespacePermission(TInfo tinfo, TCredentials credentials, String principal, String ns, byte permission, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void getConfiguration(TInfo tinfo, TCredentials credentials, ConfigurationType type, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void getTableConfiguration(TInfo tinfo, TCredentials credentials, String tableName, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void getNamespaceConfiguration(TInfo tinfo, TCredentials credentials, String ns, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void checkClass(TInfo tinfo, TCredentials credentials, String className, String interfaceMatch, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void checkTableClass(TInfo tinfo, TCredentials credentials, String tableId, String className, String interfaceMatch, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void checkNamespaceClass(TInfo tinfo, TCredentials credentials, String namespaceId, String className, String interfaceMatch, AsyncMethodCallback resultHandler) throws TException {

    }

}
