package org.jihu;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.util.ConfigUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ReconfigApp {

    private static final String CLUSTER_CONNECT_STR = "192.168.131.171:2181,192.168.131.171:2182,192.168.131.171:2183,192.168.131.171:2184";

    private static final Integer SESSION_TIMEOUT = 60 * 1000;

    private static ZooKeeper zookeeper = null;

    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.None
                    && event.getState() == Event.KeeperState.SyncConnected) {
                countDownLatch.countDown();
                log.info(" 连接建立");
                // start to watch config
                try {
                    log.info(" 开始监听配置文件节点'/zookeeper/config'：{}", ZooDefs.CONFIG_NODE);
                    // 设置为watch:true，表示把创建zk时传入的watch作为一个watch
                    //  zookeeper.getConfig用于获取配置信息并进行监听
                    zookeeper.getConfig(true, null);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            } else if (event.getPath() != null && event.getPath().equals(ZooDefs.CONFIG_NODE)) {
                try {
                    byte[] config = zookeeper.getConfig(this, null);
                    String clientConfigStr = ConfigUtils.getClientConfigStr(new String(config));
                    log.info(" 配置发生变更: {}", clientConfigStr);
                    // 动态刷新服务列表 
                    zookeeper.updateServerList(clientConfigStr.split(" ")[1]);
                } catch (KeeperException | InterruptedException | IOException e) {
                    e.printStackTrace();
                }

            }
        }
    };

    // 初始化zk
    @Before
    public void init() throws IOException {
        log.info("try to connect to zk server");
        zookeeper = new ZooKeeper(CLUSTER_CONNECT_STR, SESSION_TIMEOUT, watcher);
    }


    @After
    public void holdOnzk() throws InterruptedException {
        // 不要让程序结束. 然后我们在客户端中去修改数据
        TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
    }
}
