package org.jihu;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;

@Slf4j
public class CuratorTest {
    private static final String CONNECT_STR = "192.168.131.171";

    private static final Integer CONNECTION_TIMEOUT_MS = 5000;

    private static CuratorFramework curatorFramework;


    @Before
    public void init () {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(5000, 30);

        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_STR)
                .sessionTimeoutMs(CONNECTION_TIMEOUT_MS)
                .canBeReadOnly(true)
                .retryPolicy(retryPolicy)
                .build();

        curatorFramework.getConnectionStateListenable().addListener((client, newState) -> {
            if (newState == ConnectionState.CONNECTED) {
                log.info("连接成功！");
            }
        });

        log.info("连接中...");

        curatorFramework.start();
    }



    public void createIfNeed(String path) {
        try {
            Stat stat = curatorFramework.checkExists().forPath(path);
            if (stat == null) {
                String s = curatorFramework.create().forPath(path);
                log.info("path {} created", s);
            }
        } catch (Exception e) {
            log.error("节点{}创建失败. {}", path, e);
        }
    }

    @Test
    public void getData() {
        try {
            byte[] bytes = curatorFramework.getData().forPath("/w-test");
            String s = transData(bytes);
            log.info("data: {}", s);
        } catch (Exception e) {
            log.info("获取失败...");
        }

    }

    private String transData(byte[] data) {
        if (null != data) {
            return new String(data);
        } else {
            return null;
        }
    }

    @Test
    public void testCreateNode() throws Exception {
        String path = curatorFramework.create().forPath("/t-curator-node", "data".getBytes());
        String result = new String(curatorFramework.getData().forPath("/t-curator-node"));
        log.info("node created successfully. Current value{}", result);
    }

    /**
     * 递归创建子节点
     */
    @Test
    public void testCreateWithParent() {
        String pathWithParent = "/node-parent/sub-node-1";

        String path = null;
        try {
            path = curatorFramework.create().creatingParentContainersIfNeeded().forPath(pathWithParent);
            log.info("curator create node {} successfully.", path);
        } catch (Exception e) {
            log.info("Faild to create node {}. {}{", path, e);
        }

    }

    /**
     * 递归安全删除节点
     * @throws Exception
     */
    @Test
    public void testDelete() throws Exception {
        curatorFramework.delete()
                /*
                Solves edge cases where an operation may succeed on the server but connection failure occurs before a
                * response can be successfully returned to the client.
                 */
                .guaranteed()  // 防止客户端异常导致删除不成功
                .deletingChildrenIfNeeded()  // 遍历删除子节点
                .forPath("/node-parent/sub-node-1");

    }

    /**
     * 保护模式，防止由于异常原因，导致僵尸节点
     *
     * @throws Exception
     */
    @Test
    public void testCreateProtection() throws Exception {
        String path = curatorFramework.create()
                .withProtection()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath("/curator-protection-node", "data".getBytes());

        log.info("curator create node {} successfully.", path);
    }

    @Test
    public void testSet() {
        try {
            Stat stat = curatorFramework.setData().forPath("/curator-node", "changed-data".getBytes());
            byte[] bytes = curatorFramework.getData().forPath("/curator-node");
            String nenwResult = new String(bytes);
            log.info("new data: {}", nenwResult);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void assignThreadPool() {
        ArrayBlockingQueue blockingQueue = new ArrayBlockingQueue(50);

        RejectedExecutionHandler abortPolicy = new ThreadPoolExecutor.AbortPolicy();
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(5, 5, 0L, TimeUnit.SECONDS, blockingQueue, abortPolicy);

        byte[] bytes = new byte[0];
        try {
            curatorFramework.getData().inBackground(((client, event) -> {
                log.info("background: {}", event);
                log.info("data:{}", new String(event.getData()));
            }), threadPoolExecutor).forPath("/zk-node");
        } catch (Exception e) {
            log.error("Faild to get data: {}", e);
        }
    }

    @After
    public void holdOnzk() throws InterruptedException {
        // 不要让程序结束. 然后我们在客户端中去修改数据
        TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
    }

}
