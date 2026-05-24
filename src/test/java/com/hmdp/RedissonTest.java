package com.hmdp;

import com.hmdp.HmDianPingApplication;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootTest
public class RedissonTest {
    @Resource
    private RedissonClient redissonClient;

    @Resource
    private RedissonClient redissonClient2;

    private RLock rLock;

    @BeforeEach
    void setUp(){
        RLock rLock1 = redissonClient.getLock("order");
        RLock rLock2 = redissonClient2.getLock("order");

        // 创建联锁
        rLock = redissonClient.getMultiLock(rLock1, rLock2); // 内部是new，用哪个都行
    }

    @Test
    public void method1() throws InterruptedException {
        boolean isLock = rLock.tryLock(1L, TimeUnit.SECONDS);
        if (!isLock){
            log.info("获取锁失败--1");
        }
        try {
            log.info("获取锁成功--1");
            method2();
            log.info("执行业务--1");
        }
        finally {
            log.info("准备释放锁--1");
            rLock.unlock();
        }
    }

    public void method2(){
        boolean isLock = rLock.tryLock();
        if (!isLock){
            log.info("获取锁失败--2");
        }
        try {
            log.info("获取锁成功--2");
            log.info("执行业务--2");
        }
        finally {
            log.info("准备释放锁--2");
            rLock.unlock();
        }
    }
}