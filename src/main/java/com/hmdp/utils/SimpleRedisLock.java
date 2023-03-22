package com.hmdp.utils;

import cn.hutool.core.lang.Range;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements Ilock{

    private String name;
    private StringRedisTemplate stringRedisTemplate;
    private static final String KEY_PREFIX = "lock";

    @Override
    public boolean trylock(long timeSec) {

        long threadid = Thread.currentThread().getId();

        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + name,threadid + "",timeSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void setlock() {

        stringRedisTemplate.delete(KEY_PREFIX + name);

    }
}
