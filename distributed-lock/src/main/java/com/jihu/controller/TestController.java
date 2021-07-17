package com.jihu.controller;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


@RestController
public class TestController {


    @Autowired
    private OrderService orderService;

    @Value("${server.port}")
    private String port;

    @Autowired
    CuratorFramework curatorFramework;

    @GetMapping("/stock/deduct")
    public Object reduceStock(Integer id) throws Exception {

        // 创建一个互斥锁
        InterProcessMutex interProcessMutex = new InterProcessMutex(curatorFramework, "/product_" + id);

        InterProcessReadWriteLock readWriteLock = new InterProcessReadWriteLock(curatorFramework, "/product_" + id);
        readWriteLock.readLock().acquire();
        readWriteLock.writeLock().acquire();

        try {
            // ...
            // 加锁后其他线程要一直等待
            interProcessMutex.acquire();
            // 执行一段时间
            // interProcessMutex.acquire(5, TimeUnit.SECONDS);
            orderService.reduceStock(id);

        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw e;
            }
        }finally {
            interProcessMutex.release();
        }
        return "ok:" + port;
    }


}

