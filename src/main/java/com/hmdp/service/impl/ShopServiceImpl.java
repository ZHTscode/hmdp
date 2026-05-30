package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        // 缓存穿透
        // Shop shop = queryWithPassThrough(id);
        // Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 互斥锁解决缓存击穿
        // Shop shop = queryWithMutex(id);

        // 逻辑过期解决缓存击穿
        // Shop shop = queryWithLogicalExpire(id);
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 20L, TimeUnit.SECONDS);

        if (shop == null) {
            return Result.fail("店铺不存在!");
        }

        return Result.ok(shop);
    }

    public Shop queryWithPassThrough(Long id){
        String key = CACHE_SHOP_KEY + id;
        // 1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.如果存在，返回商铺信息
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 判断命中是否为空值
        if(shopJson != null){
            return null;
        }
        // 3.如果不存在，从数据库查询
        Shop shop = getById(id);
        // 4.如果数据库不存在，返回错误
        if (shop == null) {
            // 将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 5.如果数据库存在，写入redis缓存，返回商铺信息
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);

        return shop;
    }

    public Shop queryWithMutex(Long id){
        String key = CACHE_SHOP_KEY + id;
        // 1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.如果存在，返回商铺信息
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 判断命中是否为空值
        if (shopJson != null) {
            return null;
        }
        // 3.实现缓存重建
        String lockKey = LOCK_SHOP_KEY + id; // 3.1.获取互斥锁
        Shop shop = null;
        try {
            boolean isLocked = tryLock(lockKey);
            if (!isLocked) { // 3.2.失败，休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            // 4.如果获取成功，查询数据库
            shop = getById(id);
            Thread.sleep(200); // 模拟数据库查询耗时
            // 5.如果数据库不存在，返回错误
            if (shop == null) {
                // 将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 6.如果数据库存在，写入redis缓存，返回商铺信息
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            // 7.释放互斥锁
            unlock(lockKey);
        }
        return shop;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    private Shop queryWithLogicalExpire(Long id) {
        String shopKey = CACHE_SHOP_KEY + id;
        // 1.从redis中查询
        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
        // 2.判断是否存在
        if (StrUtil.isBlank(shopJson)) {
            // 不存在返回空
            return null;
        }
        // 3.命中，把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 4.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 4.1.未过期，直接返回
            return shop;
        }
        // 4.2.已过期
        // 4.3.获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 4.4.是否获取锁成功
        if (isLock) {
            // 4.5.成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    this.saveShop2Redis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally { //释放锁
                    unlock(lockKey);
                }
            });
        }
        // 5.返回过期的商铺信息
        return shop;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        // 1.查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200); // 模拟查询耗时
        // 2.封装逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 3.写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺id不能为空");
        }
        // 1.更新数据库
        updateById(shop);
        // 2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 判断是否根据坐标查询
        if (x == null || y == null) {
            // 不根据坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        // 计算分页面参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        // 根据坐标查询
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(
                key,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
        );
        if (results == null) {
            return Result.ok(Collections.emptyList()); // 返回空集合
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页了，返回空集合
            return Result.ok(Collections.emptyList());
        }
        // 截取分页数据
        List<Long> ids = new ArrayList<>(list.size()); // 创建一个列表用于存储店铺ID
        Map<Long, Distance> distanceMap = new HashMap<>(list.size()); // 创建一个映射用于存储店铺ID和距离
        list.stream().skip(from).forEach(result -> {
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr)); // 添加店铺ID到列表
            Distance distance = result.getDistance();
            distanceMap.put(Long.valueOf(shopIdStr), distance);
        });
        // 根据店铺ID查询店铺数据
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id, " + idStr + ")").list();
        // 设置距离
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId()).getValue());
        }
        // 返回数据
        return Result.ok(shops);
    }
}
