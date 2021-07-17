package org.jihu;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.jihu.config.MyConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ZkTest1 {

    private static final String CONNECT_STR = "192.168.131.171";

    private static final Integer SESSION_TIMEOUT = 30 * 1000;

    private static ZooKeeper zooKeeper = null;

    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private static ObjectMapper objectMapper = new ObjectMapper();

    // 初始化zk
    @Before
    public void init() throws IOException {
        log.info("try to connect to zk server");
        zooKeeper = new ZooKeeper(CONNECT_STR, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 根据事件类型判断是否连接成功
                if (event != null && event.getType().equals(Watcher.Event.EventType.None)
                        && event.getState().equals(Watcher.Event.KeeperState.SyncConnected)) {
                    // 当连接建立好之后
                    countDownLatch.countDown();
                    log.info("zk 连接成功...");
                }
            }
        });
    }


    /**
     * 循环监听
     */
    private Watcher createDataChangeWatch(String path) {
        return new Watcher() {
            @SneakyThrows
            @Override
            public void process(WatchedEvent event) {
                // 获取到监听事件之后
                // 判断路径是否是配置路径，并且事件类型是NodeDataChanged
                if (path.equals(event.getPath())
                        && Watcher.Event.EventType.NodeDataChanged.equals(event.getType())) {
                    log.info("PATH: {}, 发生了数据变化.", event.getType());
                    // 获取数据并添加监听
                    byte[] data = zooKeeper.getData(path, this, null);
                    log.info("数据发生变化: {}", objectMapper.readValue(new String(data), MyConfig.class));
                }
            }
        };
    }

    @Test
    public void getConfig() throws KeeperException, InterruptedException, JsonProcessingException {
        String path = "/myconfig";
        byte[] data = zooKeeper.getData(path, createDataChangeWatch(path), null);
        log.info("data: {}", objectMapper.readValue(new String(data), MyConfig.class));
    }


    @Test
    public void createTest() throws KeeperException, InterruptedException {
        String zkNode = "/t_create1";
        String path = zooKeeper.create(zkNode, "data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        log.info("created path: {}",path);
    }


    @Test
    public void optimisticUpdate() {
        String zkNode = "/t_optimisticUpdate";
        // 先创建一个节点
        try {
            zooKeeper.create(zkNode, "data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
            log.error("节点创建失败： {}", e);
        }
        // 获取节点信息包括version
        Stat stat = new Stat();
        try {
            byte[] data = zooKeeper.getData(zkNode, false, stat);
        } catch (KeeperException | InterruptedException e) {
            log.error("节点获取失败： {}", e);
        }

        int version1 = stat.getVersion();
        log.info("第一次的版本号：{}", version1);

        // 乐观锁修改
        try {
            Stat stat1 = zooKeeper.setData(zkNode, "data2".getBytes(), version1);
            log.info("第一次修改成功.版本号：{}", version1);
        } catch (KeeperException | InterruptedException e) {
            log.error("修改节点失败. version {}： {}", version1, e.getMessage());
        }
        // 乐观锁修改2, 此时传入旧的版本号，会修改失败
        try {
            Stat stat2 = zooKeeper.setData(zkNode, "data3".getBytes(), version1);
        } catch (KeeperException | InterruptedException e) {
            log.error("修改节点失败. version {}： {}", version1, e.getMessage());
        }

        byte[] data1 = new byte[0];
        try {
            data1 = zooKeeper.getData(zkNode, false, stat);
            int version2 = stat.getVersion();
            log.info("当前数据数据是：{}. version: {}", new String(data1), version2);
        } catch (KeeperException | InterruptedException e) {
            log.error("节点获取失败： {}", e);
        }
    }

    @Test
    public void testDelete() {
        String zkNode = "/t_optimisticUpdate";
        try {
            zooKeeper.delete(zkNode, -1);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createAsycTest() throws InterruptedException {
        String zkNode = "/t_createAsyc";
        zooKeeper.create(zkNode, "data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT, (rc, path, ctx, name) -> {
                    String threadName = Thread.currentThread().getName();
                    log.info("threadName:{}, rc  {},path {},ctx {},name {}",threadName, rc,path,ctx,name);

                }, "context");

        log.info("over");
    }

    @After
    public void holdOnzk() throws InterruptedException {
        // 不要让程序结束. 然后我们在客户端中去修改数据
        TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
    }
}
