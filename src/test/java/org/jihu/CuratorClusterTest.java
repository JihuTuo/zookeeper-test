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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CuratorClusterTest {
    private static final String CLUSTER_CONNECT_STR = "192.168.131.171:2181,192.168.131.171:2182,192.168.131.171:2183,192.168.131.171:2184";

    private static final Integer CONNECTION_TIMEOUT_MS = 5000;

    private static CuratorFramework curatorFramework;


    @Before
    public void init () {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(5000, 30);

        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(CLUSTER_CONNECT_STR)
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


    @Test
    public void testCluster() throws Exception {
        String path = "/test-curator-cluster";
        byte[] data  = curatorFramework.getData().forPath(path);
        log.info("get data: {}", new String(data));

        // 不断的获取数据
        while (true) {
            try {
                byte[] bytes = curatorFramework.getData().forPath(path);
                log.info("get data 2 {}", new String(bytes));

                TimeUnit.SECONDS.sleep(5);
            } catch (Exception e) {
                log.error("获取数据异常： {}", e.getMessage());
                testCluster();
            }
        }
    }



    @After
    public void holdOnzk() throws InterruptedException {
        // 不要让程序结束. 然后我们在客户端中去修改数据
        TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
    }

}
