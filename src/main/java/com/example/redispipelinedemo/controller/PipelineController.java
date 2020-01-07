package com.example.redispipelinedemo.controller;

import java.util.HashMap;
import java.util.ArrayList;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @Author: 狂奔的蜗牛【email:zhouguanya20@163.com】
 * @Date: 2020-01-07 00:19
 * @Description: 测试Pipeline命令
 */
@RestController
public class PipelineController {
    @Autowired
    StringRedisTemplate stringRedisTemplate;

    /**
     * 不使用Pipeline命令
     *
     * @param count 操作的命令个数
     * @return 执行时间
     */
    @GetMapping("redis/no/pipeline/{count}")
    public String testWithNoPipeline(@PathVariable("count") String count) {
        // 开始时间
        long start = System.currentTimeMillis();
        // 参数校验
        if (StringUtils.isEmpty(count)) {
            throw new IllegalArgumentException("参数异常");
        }
        // for循环执行N次Redis操作
        for (int i = 0; i < Integer.parseInt(count); i++) {
            // 设置K-V
            stringRedisTemplate.opsForValue().set(String.valueOf(i),
                    String.valueOf(i), 1, TimeUnit.HOURS);
        }
        // 结束时间
        long end = System.currentTimeMillis();
        // 返回总执行时间
        return "执行时间等于=" + (end - start) + "毫秒";
    }


    /**
     * 使用Pipeline命令
     *
     * @param count 操作的命令个数
     * @return 执行时间
     */
    @GetMapping("redis/pipeline/{count}")
    public String testWithPipeline(@PathVariable("count") String count) {
        // 开始时间
        long start = System.currentTimeMillis();
        // 参数校验
        if (StringUtils.isEmpty(count)) {
            throw new IllegalArgumentException("参数异常");
        }
        /* 插入多条数据 */
        stringRedisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            public <K, V> Object execute(RedisOperations<K, V> redisOperations) throws DataAccessException {
                for (int i = 0; i < Integer.parseInt(count); i++) {
                    stringRedisTemplate.opsForValue().set(String.valueOf(i), String.valueOf(i), 1, TimeUnit.HOURS);
                }
                return null;
            }
        });
        // 结束时间
        long end = System.currentTimeMillis();
        // 返回总执行时间
        return "执行时间等于=" + (end - start) + "毫秒";
    }

    /**
     * 不使用Pipeline命令单条执行get命令
     *
     * @param count 操作的命令个数
     * @return 执行时间
     */
    @GetMapping("redis/no/pipeline/get/{count}")
    public Map<String, Object> testGetWithNoPipeline(@PathVariable("count") String count) {
        // 开始时间
        long start = System.currentTimeMillis();
        // 参数校验
        if (StringUtils.isEmpty(count)) {
            throw new IllegalArgumentException("参数异常");
        }
        List<String> resultList = new ArrayList<>();
        // for循环执行N次Redis操作
        for (int i = 0; i < Integer.parseInt(count); i++) {
            // 获取K-V
            resultList.add(stringRedisTemplate.opsForValue().get(String.valueOf(i)));
        }
        // 结束时间
        long end = System.currentTimeMillis();
        Map<String, Object> resultMap = new HashMap<>(4);
        resultMap.put("执行时间", (end - start) + "毫秒");
        resultMap.put("执行结果", resultList);
        // 返回resultMap
        return resultMap;
    }

    /**
     * 使用Pipeline命令
     *
     * @param count 操作的命令个数
     * @return 执行时间
     */
    @GetMapping("redis/pipeline/get/{count}")
    public Map<String, Object> testGetWithPipeline(@PathVariable("count") String count) {
        // 开始时间
        long start = System.currentTimeMillis();
        // 参数校验
        if (StringUtils.isEmpty(count)) {
            throw new IllegalArgumentException("参数异常");
        }
        // for循环执行N次Redis操作
        /* 批量获取多条数据 */
        List<Object> resultList = stringRedisTemplate.executePipelined(new RedisCallback<String>() {
            @Override
            public String doInRedis(RedisConnection redisConnection) throws DataAccessException {
                StringRedisConnection stringRedisConnection = (StringRedisConnection) redisConnection;
                for (int i = 0; i < Integer.parseInt(count); i++) {
                    stringRedisConnection.get(String.valueOf(i));
                }
                return null;
            }
        });
        // 结束时间
        long end = System.currentTimeMillis();
        Map<String, Object> resultMap = new HashMap<>(4);
        resultMap.put("执行时间", (end - start) + "毫秒");
        resultMap.put("执行结果", resultList);
        // 返回resultMap
        return resultMap;
    }
}
