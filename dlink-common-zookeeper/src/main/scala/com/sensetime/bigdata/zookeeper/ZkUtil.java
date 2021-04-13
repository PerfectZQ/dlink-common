package com.sensetime.bigdata.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.atomic.PromotedToLock;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhangqiang, created on 2020/5/14 19:11
 */
public class ZkUtil {

    private static Logger log = LoggerFactory.getLogger(ZkUtil.class);

    private static final String zkHosts;

    // Zookeeper 根目录的 namespace
    private static final String zkNamespace;

    // Server 默认 Zk 会话
    private static final CuratorFramework defaultClient;
    // public static final Subject subject;

    static {
        Properties props = new Properties();
        try {
            props.load(ZkUtil.class.getClassLoader().getResourceAsStream("zookeeper.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        zkHosts = props.getProperty("zookeeper.hosts");
        zkNamespace = props.getProperty("zookeeper.namespace");
        String username = props.getProperty("zookeeper.username");
        String password = props.getProperty("zookeeper.password");
        // 配置 JAAS
        Configuration.setConfiguration(new ZkJaasConfiguration(username, password));
        defaultClient = createZkClient();
        defaultClient.start();
    }

    public static String getZkHosts() {
        return zkHosts;
    }

    public static String getZkNamespace() {
        return zkNamespace;
    }

    public static CuratorFramework getDefaultClient() {
        return defaultClient;
    }

    /**
     * 创建一个新的 ZkClient
     *
     * @return
     */
    public static CuratorFramework createZkClient() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        return CuratorFrameworkFactory.builder()
                /*
                .zookeeperFactory(new ZookeeperFactory() {
                    @Override
                    public ZooKeeper newZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws Exception {
                        ZKClientConfig config = new ZKClientConfig();
                        // 配置 ZK Config
                        config.setProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY, "true");
                        return new ZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly, config);
                    }
                })
                */
                .connectString(zkHosts)
                // 连接会话的超时时间，默认15000ms
                .sessionTimeoutMs(15000)
                .retryPolicy(retryPolicy)
                // 该客户端的所有节点路径都是相对于`/zkNamespace`来的
                .namespace(zkNamespace)
                .build();
    }


    /**
     * 包含所有服务启动必须的 ZkNode
     */
    public enum ZkNode {

        // 客户端心跳汇报
        CLIENT_HEARTBEAT("/client_heartbeat", CreateMode.PERSISTENT),
        // 所有分布式锁路径
        DISTRIBUTED_LOCKS("/locks", CreateMode.PERSISTENT),
        // 分布式 Counter
        DISTRIBUTED_COUNTER("/counter", CreateMode.PERSISTENT);

        private String path;

        private String data;

        private CreateMode createMode;

        public String getPath() {
            return path;
        }

        public String getData() {
            return data;
        }

        public CreateMode getCreateMode() {
            return createMode;
        }

        ZkNode(String path, String data, CreateMode createMode) {
            this.path = path;
            this.data = data;
            this.createMode = createMode;
        }

        ZkNode(String path, CreateMode createMode) {
            this(path, "", createMode);
        }

        /**
         * 获取 Node 在 Zookeeper 中真实的绝对路径(包含 zkNamespace)
         *
         * @return
         */
        public String getAbsolutePath() {
            return String.format("/%s%s", zkNamespace, path);
        }

        @Override
        public String toString() {
            return this.name() + "(absolutePath: '" + getAbsolutePath() + "', data: '" + data + "', createMode: '" + createMode + "')";
        }

    }

    /**
     * 服务器启动时检查或初始化所有必须的 ZkNode
     */
    public static void initZkNodes() throws Exception {
        log.info("Initialize all must ZkNodes.");
        ZkNode[] zkNodes = ZkNode.values();
        final AtomicBoolean result = new AtomicBoolean(false);
        final CountDownLatch semaphore = new CountDownLatch(zkNodes.length);
        if (defaultClient.getState() == CuratorFrameworkState.LATENT ||
                defaultClient.getState() == CuratorFrameworkState.STOPPED) {
            defaultClient.start();
            log.info("Zookeeper defaultClient start.");
        }
        for (ZkNode zkNode : zkNodes) {
            final String nodePath = zkNode.getPath();
            final CreateMode createMode = zkNode.getCreateMode();
            BackgroundCallback bgc = (client, event) -> {
                // 输出节点创建情况
                int resultCode = event.getResultCode();
                if (nodePath.equals(event.getPath())) {
                    log.info("Create ZkNode - " + zkNode + ", Result: " + KeeperException.Code.get(resultCode));
                    result.set(resultCode == KeeperException.Code.NODEEXISTS.intValue() ||
                            resultCode == KeeperException.Code.OK.intValue());
                    semaphore.countDown();
                }
            };
            defaultClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(createMode)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .inBackground(bgc)
                    .forPath(zkNode.getPath(), zkNode.getData().getBytes());
        }
        semaphore.await(60, TimeUnit.SECONDS);
        if (!result.get()) {
            throw new RuntimeException("Initialize must ZkNodes failed");
        }
    }


    /**
     * 检测节点是否存在
     *
     * @param path 指定节点路径
     * @return true 存在
     * @throws Exception
     */
    public static boolean isNodeExists(String path) throws Exception {
        Stat stat = defaultClient.checkExists().forPath(path);
        return stat != null;
    }

    /**
     * 获取指定节点的数据
     *
     * @param path
     * @return
     * @throws Exception
     */
    public static String getNodeData(String path) throws Exception {
        try (CuratorFramework client = createZkClient()) {
            client.start();
            byte[] byteData = client.getData().forPath(path);
            return new String(byteData);
        }
    }

    /**
     * 获取指定节点的一级子节点
     *
     * @param path
     * @return
     * @throws Exception
     */
    public static List<String> getChildren(String path) throws Exception {
        try (CuratorFramework client = createZkClient()) {
            client.start();
            return client.getChildren().forPath(path);
        }
    }

    /**
     * 创建一个持久化的 ZkNode
     *
     * @param path
     * @throws Exception
     */
    public static void createPersistentNode(String path) throws Exception {
        try (CuratorFramework client = createZkClient()) {
            client.start();
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path);
        } catch (KeeperException.NodeExistsException e) {
            // Do Nothing
        }
    }

    /**
     * 创建一个持久化的 ZkNode
     *
     * @param path
     * @throws Exception
     */
    public static void createPersistentNode(String path, String data) throws Exception {
        try (CuratorFramework client = createZkClient()) {
            client.start();
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, data.getBytes());
        } catch (KeeperException.NodeExistsException e) {
            // Do Nothing
        }
    }

    /**
     * 创建一个临时节点
     *
     * @param client 需要自己维护 client 状态，如果 client 关闭了，临时节点就会消失
     * @param path
     * @throws Exception
     */
    public static void createTemporaryNode(CuratorFramework client, String path) throws Exception {
        if (client.getState() == CuratorFrameworkState.LATENT ||
                client.getState() == CuratorFrameworkState.STOPPED) {
            client.start();
        }
        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(path);
    }

    /**
     * 创建一个临时节点
     *
     * @param client 需要自己维护 client 状态，如果 client 关闭了，临时节点就会消失
     * @param path
     * @param data
     * @throws Exception
     */
    public static void createTemporaryNode(CuratorFramework client, String path, String data) throws Exception {
        if (client.getState() == CuratorFrameworkState.LATENT ||
                client.getState() == CuratorFrameworkState.STOPPED) {
            client.start();
        }
        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(path, data.getBytes());
    }

    /**
     * 修改 ZkNode 节点信息
     *
     * @param path
     * @param data
     */
    public static void setData(String path, String data) {
        try (CuratorFramework client = createZkClient()) {
            client.start();
            client.setData().forPath(path, data.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取指定路径的分布式自增 ID
     *
     * @param path
     * @return
     * @throws Exception
     */
    public static long incrementAndGetDistributedLong(String path) throws Exception {
        try (CuratorFramework client = createZkClient()) {
            client.start();
            String counterPath = ZkNode.DISTRIBUTED_COUNTER.getPath() + "/" + path;
            PromotedToLock promotedToLock = PromotedToLock.builder()
                    .lockPath(ZkNode.DISTRIBUTED_LOCKS.getPath() + "/atomic_counter/" + path).build();
            DistributedAtomicLong distributedAtomicCounter = new DistributedAtomicLong(client,
                    counterPath, new ExponentialBackoffRetry(1000, 10),
                    promotedToLock);
            AtomicValue<Long> v = distributedAtomicCounter.increment();
            while (!v.succeeded()) {
                v = distributedAtomicCounter.increment();
            }
            return v.postValue();
        }
    }


    /**
     * 注册监听事件
     *
     * @param client
     * @param path
     * @param watcher
     * @throws Exception
     */
    public static void registerWatcher(CuratorFramework client, String path, Watcher watcher) throws Exception {
        client.getData().usingWatcher(watcher).forPath(path);
    }


    public static void main(String[] args) throws Exception {
        log.error("=====> Error Test");
        createTemporaryNode(defaultClient, "/test");
        Thread.sleep(1000 * 10);
    }
}