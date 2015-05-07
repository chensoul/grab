package com.javachen.grab.common.zk;

import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import com.javachen.grab.common.io.IOUtils;
import com.javachen.grab.common.lang.JVMUtils;


/**
 * This class contains code copied from Zookeeper's QuorumPeerMain and ZooKeeperServerMain.
 * It runs a local instance of Zookeeper, which can be useful for testing.
 */
public final class LocalZKServer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(LocalZKServer.class);

    private final int port;
    private Path dataDir;
    private DatadirCleanupManager purgeManager;
    private ZooKeeperServer zkServer;
    private FileTxnSnapLog transactionLog;
    private ServerCnxnFactory connectionFactory;
    private boolean closed;

    /**
     * Creates an instance that will run Zookeeper on the given port.
     *
     * @param port port on which Zookeeper will listen
     */
    public LocalZKServer(int port) {
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    /**
     * Starts Zookeeper.
     *
     * @throws IOException          if an error occurs during initialization
     * @throws InterruptedException if an error occurs during initialization
     */
    public synchronized void start() throws IOException, InterruptedException {
        log.info("Starting Zookeeper on port {}", port);

        dataDir = Files.createTempDirectory(LocalZKServer.class.getSimpleName());
        dataDir.toFile().deleteOnExit();

        Properties properties = new Properties();
        properties.setProperty("dataDir", dataDir.toAbsolutePath().toString());
        properties.setProperty("clientPort", Integer.toString(port));
        log.info("ZK config: {}", properties);

        QuorumPeerConfig quorumConfig = new QuorumPeerConfig();
        try {
            quorumConfig.parseProperties(properties);
        } catch (QuorumPeerConfig.ConfigException e) {
            throw new IllegalArgumentException(e);
        }

        purgeManager =
                new DatadirCleanupManager(quorumConfig.getDataDir(),
                        quorumConfig.getDataLogDir(),
                        quorumConfig.getSnapRetainCount(),
                        quorumConfig.getPurgeInterval());
        purgeManager.start();

        ServerConfig serverConfig = new ServerConfig();
        serverConfig.readFrom(quorumConfig);

        zkServer = new ZooKeeperServer();
        zkServer.setTickTime(serverConfig.getTickTime());
        zkServer.setMinSessionTimeout(serverConfig.getMinSessionTimeout());
        zkServer.setMaxSessionTimeout(serverConfig.getMaxSessionTimeout());

        // These two ServerConfig methods returned String in 3.4.x and File in 3.5.x
        transactionLog = new FileTxnSnapLog(new File(serverConfig.getDataLogDir().toString()),
                new File(serverConfig.getDataDir().toString()));
        zkServer.setTxnLogFactory(transactionLog);

        connectionFactory = ServerCnxnFactory.createFactory();
        connectionFactory.configure(serverConfig.getClientPortAddress(), serverConfig.getMaxClientCnxns());
        connectionFactory.startup(zkServer);
    }

    /**
     * Blocks until Zookeeper terminates.
     *
     * @throws InterruptedException if the caller thread is interrupted while waiting
     */
    public void await() throws InterruptedException {
        connectionFactory.join();
    }

    /**
     * Shuts down Zookeeper.
     */
    @Override
    public synchronized void close() throws IOException {
        log.info("Closing...");
        if (closed) {
            return;
        }
        closed = true;
        if (connectionFactory != null) {
            connectionFactory.shutdown();
            connectionFactory = null;
        }
        if (zkServer != null) {
            zkServer.shutdown();
            zkServer = null;
        }
        if (transactionLog != null) {
            transactionLog.close();
            transactionLog = null;
        }
        if (purgeManager != null) {
            purgeManager.shutdown();
            purgeManager = null;
        }
        if (dataDir != null) {
            IOUtils.deleteRecursively(dataDir);
            dataDir = null;
        }
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : IOUtils.chooseFreePort();
        try (final LocalZKServer zkServer = new LocalZKServer(port)) {
            JVMUtils.closeAtShutdown(zkServer);
            zkServer.start();
            zkServer.await();
        }
    }

}
