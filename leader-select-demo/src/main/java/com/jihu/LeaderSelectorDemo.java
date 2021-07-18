package com.jihu;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class LeaderSelectorDemo {


    private static final String CONNECT_STR = "192.168.133.171:2181";

    private static RetryPolicy retryPolicy = new ExponentialBackoffRetry(5000, 10);

    private static CuratorFramework curatorFramework;


    private static CountDownLatch countDownLatch = new CountDownLatch(1);


    public static void main(String[] args) throws InterruptedException {


        String appName = System.getProperty("appName");
        System.out.println("appName: " + appName);

        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(CONNECT_STR, retryPolicy);
        LeaderSelectorDemo.curatorFramework = curatorFramework;
        curatorFramework.start();

        byte[] bytes = new byte[0];
        try {
            bytes = curatorFramework.getData().watched().forPath("/test1");
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("获取数据成功" + new String(bytes));

        LeaderSelectorListener listener = new LeaderSelectorListenerAdapter() {
            @Override
            public void takeLeadership(CuratorFramework client) throws Exception {

                System.out.println(" I' m leader now . i'm , " + appName);

                TimeUnit.SECONDS.sleep(15);

            }
        };

        LeaderSelector selector = new LeaderSelector(curatorFramework, "/cachePreHeat_leader", listener);
        selector.autoRequeue();  // not required, but this is behavior that you will probably expect
        selector.start();
        countDownLatch.await();

    }
}
