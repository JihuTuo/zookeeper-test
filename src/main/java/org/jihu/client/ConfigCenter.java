package org.jihu.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.jihu.config.MyConfig;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ConfigCenter {
    private static final String CONNECT_STR = "192.168.131.171";

    private static final Integer SESSION_TIMEOUT = 30 * 1000;

    private static ZooKeeper zooKeeper = null;

    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        /**
         * Zookeeper连接成功后会触发一个事件，状态是SyncConnected，类型为None
         *  WatchedEvent state:SyncConnected type:None path:null，
         *
         *  所以我们只要监听到这个事件就说明连接成功了
         */
        // 注意，zooKeeper的建立默认是异步的，会启动其他线程去建立
        /**
         *  public void start() {
         *         sendThread.start();
         *         eventThread.start();
         *     }
         *
         *  这里的sendThread和eventThread都是守护线程，如果没有业务线程，就会退出。所以如果我们直接就这样创建，
         *  可能我们的连接还没建立起来，整个程序就结束了
         *
         *  这里我们需要使用CountDownLatch
         */
        zooKeeper = new ZooKeeper(CONNECT_STR, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 根据事件类型判断是否连接成功
                if (event != null && event.getType().equals(Watcher.Event.EventType.None)
                        && event.getState().equals(Watcher.Event.KeeperState.SyncConnected)) {
                    log.info("zk 连接成功...");
                    // 当连接建立好之后
                    countDownLatch.countDown();
                }
            }
        });

        // 阻塞直到count == 0
        countDownLatch.await();

        // 执行到此处，表明连接已经建立好了

        // ======== 将配置存储到zk中 ==========

        MyConfig config = MyConfig.builder().key("anyKey").name("anyName").build();

        ObjectMapper objectMapper = new ObjectMapper();
        byte[] configBytes = objectMapper.writeValueAsBytes(config);

        // 创建zk节点
        final String configPath = "/myconfig";
        try {
            String result = zooKeeper.create(configPath, configBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            log.error("{}", e);
        }


        /**
         * 循环监听
         */
        Watcher watcher = new Watcher() {
            @SneakyThrows
            @Override
            public void process(WatchedEvent event) {
                // 获取到监听事件之后
                // 判断路径是否是配置路径，并且事件类型是NodeDataChanged
                if (configPath.equals(event.getPath())
                        && Watcher.Event.EventType.NodeDataChanged.equals(event.getType())) {
                    log.info("PATH: {}, 发生了数据变化.", event.getType());
                    // 获取数据并添加监听
                    byte[] data = zooKeeper.getData(configPath, this, null);
                    log.info("数据发生变化: {}", objectMapper.readValue(new String(data), MyConfig.class));
                }
            }
        };

        // 获取数据并添加监听
        byte[] oldData = zooKeeper.getData(configPath, watcher, null);
        MyConfig oldConfig = objectMapper.readValue(new String(oldData), MyConfig.class);
        log.info("原始数据：{}", oldConfig);

        // 不要让程序结束. 然后我们在客户端中去修改数据
        TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
    }


}
