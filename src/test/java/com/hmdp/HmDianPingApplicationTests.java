package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
public class HmDianPingApplicationTests {

    @Resource
    private IShopService shopService;
    @Resource
    private CacheClient cacheClient;
    @Resource
    private RedisIdWorker redisIdWorker;
    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Test
    void testSaveShop() throws InterruptedException {
        Shop shop = shopService.getById(1L);
        cacheClient.setWithLogicalExpire(CACHE_SHOP_KEY + 1L, shop, 10L, TimeUnit.SECONDS);
    }

    @Test
    void testIdWorker() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300); // 开启300个线程
        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println("id = " + id);
            }
            latch.countDown();
        };
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task); // 提交任务
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("time = " + (end - begin));
    }

    @Test
    void loadShopData() {
        // 查询店铺信息
        List<Shop> list = shopService.list();
        // 店铺按typeId分组
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        // 分批写入redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            Long typeId = entry.getKey(); // 店铺类型Id
            String key = SHOP_GEO_KEY + typeId;
            List<Shop> shops = entry.getValue(); // 同类型店铺集合
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(shops.size());
            for (Shop shop : shops) {
                // stringRedisTemplate.opsForGeo().add(key, new Point(shop.getX(), shop.getY()), shop.getId().toString());
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(), shop.getY())
                ));
            }
            stringRedisTemplate.opsForGeo().add(key, locations);
        }
    }

    @Test
    void testHyperLogLog() {
        String[] values = new String[1000];
        int j = 0;
        for (int i = 0; i < 1000000; i++) {
            j = i % 1000;
            values[j] = "user_" + i;
            if (j == 999) {
                stringRedisTemplate.opsForHyperLogLog().add("hl2", values);
            }
        }
        // 统计数量
        Long count = stringRedisTemplate.opsForHyperLogLog().size("hl2");
        System.out.println("count = " + count);
    }
}
