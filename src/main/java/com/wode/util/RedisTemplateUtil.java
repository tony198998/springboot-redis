package com.wode.util;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.*;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Component
public class RedisTemplateUtil{

    @Resource(name = "redisTemplate")
    private RedisTemplate<String, String> redisTemplate;

    public RedisTemplateUtil(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }


    /**
     * 指定缓存失效时间
     *
     * @param key  键
     * @param time 时间(秒)
     * @return
     */
    public boolean expire(String key, long time) {
        try {
            if (time > 0) {
                redisTemplate.expire( key, time, TimeUnit.SECONDS);
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 根据key 获取过期时间
     *
     * @param key 键 不能为null
     * @return 时间(秒) 返回0代表为永久有效
     */
    public long getExpire(String key) {
        return redisTemplate.getExpire( key, TimeUnit.SECONDS);
    }

    /**
     * 判断key是否存在
     *
     * @param key 键
     * @return true 存在 false不存在
     */
    public boolean hasKey(String key) {
        try {
            return redisTemplate.hasKey( key);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 删除缓存
     *
     * @param key 可以传一个值 或多个
     */
    @SuppressWarnings("unchecked")
    public void del(String... key) {
        if (key != null && key.length > 0) {
            if (key.length == 1) {
                redisTemplate.delete( key[0]);
            } else {
                redisTemplate.delete(CollectionUtils.arrayToList( key));
            }
        }
    }

    /**
     * 删除缓存
     *
     * @param key 可以传一个值 或多个
     */
    @SuppressWarnings("unchecked")
    public void delBatch(String... key) {
        if (key != null && key.length > 0) {
            if (key.length == 1) {
                redisTemplate.delete( key[0]);
            } else {
                List<String> list = new ArrayList<>();
                CollectionUtils.arrayToList(key).stream().forEach(x -> list.add(String.format("%s", x)));
                redisTemplate.delete(list);
            }
        }
    }

    //============================String=============================

    /**
     * 普通缓存获取
     *
     * @param key 键
     * @return 值
     */
    public String get(String key) {
        return key == null ? null : redisTemplate.opsForValue().get( key);
    }

    /**
     * 普通缓存放入
     *
     * @param key   键
     * @param value 值
     * @return true成功 false失败
     */
    public boolean set(String key, String value) {
        try {
            redisTemplate.opsForValue().set( key, value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 普通缓存放入并设置时间
     *
     * @param key   键
     * @param value 值
     * @param time  时间(秒) time要大于0 如果time小于等于0 将设置无限期
     * @return true成功 false 失败
     */
    public boolean set(String key, String value, long time) {
        try {
            if (time > 0) {
                redisTemplate.opsForValue().set( key, value, time, TimeUnit.SECONDS);
            } else {
                set( key, value);
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public String getAndSet(String key, String value) {
        return redisTemplate.opsForValue().getAndSet( key, value);
    }

    public boolean setIfAbsent(String key, String value) {
        try {
            redisTemplate.opsForValue().setIfAbsent( key, value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean setIfAbsent(String key, String value, long time) {
        try {
            if (time > 0) {
                return redisTemplate.opsForValue().setIfAbsent( key, value, time, TimeUnit.SECONDS);
            }
            return set( key, value);
        } catch (Exception e) {
            return false;
        }
    }

    public boolean setIfPresent(String key, String value) {
        try {
            return redisTemplate.opsForValue().setIfPresent( key, value);
        } catch (Exception e) {
            return false;
        }
    }

    public boolean setIfPresent(String key, String value, long time) {
        try {
            if (time > 0) {
                redisTemplate.opsForValue().setIfPresent( key, value, time, TimeUnit.SECONDS);
            } else {
                set( key, value);
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 递增
     *
     * @param key   键
     * @param delta 要增加几(大于0)
     * @return
     */
    public long incr(String key, long delta) {
        if (delta < 0) {
            throw new RuntimeException("递增因子必须大于0");
        }
        return redisTemplate.opsForValue().increment( key, delta);
    }

    /**
     * 递减
     *
     * @param key   键
     * @param delta 要减少几(大于0)
     * @return
     */
    public long decr(String key, long delta) {
        if (delta < 0) {
            throw new RuntimeException("递减因子必须大于0");
        }
        return redisTemplate.opsForValue().increment( key, -delta);
    }

    //================================Map=================================

    /**
     * HashGet
     *
     * @param key  键 不能为null
     * @param item 项 不能为null
     * @return 值
     */
    public Object hget(String key, String item) {
        return redisTemplate.opsForHash().get( key, item);
    }

    /**
     * 获取hashKey对应的所有键值
     *
     * @param key 键
     * @return 对应的多个键值
     */
    public Map<Object, Object> hmget(String key) {
        return redisTemplate.opsForHash().entries( key);
    }

    /**
     * 获取hashKey对应的多个键值
     *
     * @param key 键
     * @return 对应的多个键值
     */
    public List<Object> hmget(String key, List<String> itemList) {
        List<Object> tmpList = new ArrayList<>(itemList);
        return redisTemplate.opsForHash().multiGet( key, tmpList);
    }

    /**
     * HashSet
     *
     * @param key 键
     * @param map 对应多个键值
     * @return true 成功 false 失败
     */
    public boolean hmset(String key, Map<String, Object> map) {
        try {
            redisTemplate.opsForHash().putAll( key, map);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * HashSet 并设置时间
     *
     * @param key  键
     * @param map  对应多个键值
     * @param time 时间(秒)
     * @return true成功 false失败
     */
    public boolean hmset(String key, Map<String, Object> map, long time) {
        try {
            redisTemplate.opsForHash().putAll( key, map);
            if (time > 0) {
                expire(key, time);
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 向一张hash表中放入数据,如果不存在将创建
     *
     * @param key   键
     * @param item  项
     * @param value 值
     * @return true 成功 false失败
     */
    public boolean hset(String key, String item, Object value) {
        try {
            redisTemplate.opsForHash().put( key, item, value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 向一张hash表中放入数据,如果不存在将创建
     *
     * @param key   键
     * @param item  项
     * @param value 值
     * @param time  时间(秒)  注意:如果已存在的hash表有时间,这里将会替换原有的时间
     * @return true 成功 false失败
     */
    public boolean hset(String key, String item, Object value, long time) {
        try {
            redisTemplate.opsForHash().put( key, item, value);
            if (time > 0) {
                //方法里边已经添加了环境变量
                expire(key, time);
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 删除hash表中的值
     *
     * @param key  键 不能为null
     * @param item 项 可以使多个 不能为null
     */
    public void hdel(String key, Object... item) {
        redisTemplate.opsForHash().delete( key, item);
    }

    /**
     * 判断hash表中是否有该项的值
     *
     * @param key  键 不能为null
     * @param item 项 不能为null
     * @return true 存在 false不存在
     */
    public boolean hHasKey(String key, String item) {
        return redisTemplate.opsForHash().hasKey( key, item);
    }

    /**
     * hash递增 如果不存在,就会创建一个 并把新增后的值返回
     *
     * @param key  键
     * @param item 项
     * @param by   要增加几(大于0)
     * @return
     */
    public double hincr(String key, String item, double by) {
        return redisTemplate.opsForHash().increment( key, item, by);
    }

    /**
     * hash递减
     *
     * @param key  键
     * @param item 项
     * @param by   要减少记(小于0)
     * @return
     */
    public double hdecr(String key, String item, double by) {
        return redisTemplate.opsForHash().increment( key, item, -by);
    }

    //============================set=============================

    /**
     * 根据key获取Set中的所有值
     *
     * @param key 键
     * @return
     */
    public Set<String> sGet(String key) {
        try {
            return redisTemplate.opsForSet().members( key);
        } catch (Exception e) {
            return null;
        }
    }
    /**
     * 根据key随机获取Set中的一个值
     *
     * @param key 键
     * @return
     */
    public String sRandomGet(String key) {
        try {
            return redisTemplate.opsForSet().randomMember( key);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 判断是否在值是否在set中包含
     *
     * @param key 键
     * @return
     */
    public Boolean sIsMember(String key, String value) {
        try {
            return redisTemplate.opsForSet().isMember( key, value);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 根据value从一个set中查询,是否存在
     *
     * @param key   键
     * @param value 值
     * @return true 存在 false不存在
     */
    public boolean sHasKey(String key, Object value) {
        try {
            return redisTemplate.opsForSet().isMember( key, value);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 将数据放入set缓存
     *
     * @param key    键
     * @param values 值 可以是多个
     * @return 成功个数
     */
    public long sSet(String key, String... values) {
        try {
            return redisTemplate.opsForSet().add( key, values);
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * 将set数据放入缓存
     *
     * @param key    键
     * @param time   时间(秒)
     * @param values 值 可以是多个
     * @return 成功个数
     */
    public long sSet(String key, long time, String... values) {
        try {
            Long count = redisTemplate.opsForSet().add( key, values);
            if (time > 0) {
                expire(key, time);
            }
            return count;
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * 获取set缓存的长度
     *
     * @param key 键
     * @return
     */
    public long sGetSetSize(String key) {
        try {
            return redisTemplate.opsForSet().size( key);
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * 移除值为value的
     *
     * @param key    键
     * @param values 值 可以是多个
     * @return 移除的个数
     */
    public long sRemove(String key, String... values) {
        try {
            return redisTemplate.opsForSet().remove( key, values);
        } catch (Exception e) {
            return 0;
        }
    }
    //===============================list=================================

    /**
     * 获取list缓存的内容
     *
     * @param key   键
     * @param start 开始
     * @param end   结束  0 到 -1代表所有值
     * @return
     */
    public List<String> lGet(String key, long start, long end) {
        try {
            return redisTemplate.opsForList().range( key, start, end);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 获取list缓存的长度
     *
     * @param key 键
     * @return
     */
    public long lGetListSize(String key) {
        try {
            return redisTemplate.opsForList().size( key);
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * 通过索引 获取list中的值
     *
     * @param key   键
     * @param index 索引  index>=0时， 0 表头，1 第二个元素，依次类推；index<0时，-1，表尾，-2倒数第二个元素，依次类推
     * @return
     */
    public Object lGetIndex(String key, long index) {
        try {
            return redisTemplate.opsForList().index( key, index);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 将list放入缓存
     *
     * @param key   键
     * @param value 值
     * @return
     */
    public boolean lSet(String key, String value) {
        try {
            redisTemplate.opsForList().rightPush( key, value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 将list放入缓存
     *
     * @param key   键
     * @param value 值
     * @param time  时间(秒)
     * @return
     */
    public boolean lSet(String key, String value, long time) {
        try {
            redisTemplate.opsForList().rightPush( key, value);
            if (time > 0) {
                expire(key, time);
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 将list放入缓存
     *
     * @param key   键
     * @param value 值
     * @return
     */
    public boolean lSet(String key, List<String> value) {
        try {
            redisTemplate.opsForList().rightPushAll( key, value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 将list放入缓存
     *
     * @param key   键
     * @param value 值
     * @param time  时间(秒)
     * @return
     */
    public boolean lSet(String key, List<String> value, long time) {
        try {
            redisTemplate.opsForList().rightPushAll( key, value);
            if (time > 0) {
                expire(key, time);
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 根据索引修改list中的某条数据
     *
     * @param key   键
     * @param index 索引
     * @param value 值
     * @return
     */
    public boolean lUpdateIndex(String key, long index, String value) {
        try {
            redisTemplate.opsForList().set( key, index, value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 移除N个值为value
     *
     * @param key   键
     * @param count 移除多少个
     * @param value 值
     * @return 移除的个数
     */
    public long lRemove(String key, long count, Object value) {
        try {
            Long remove = redisTemplate.opsForList().remove( key, count, value);
            return remove;
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * 模糊查询获取key值
     *
     * @param pattern
     * @return
     */
    public Set keys(String pattern) {
        return redisTemplate.keys( pattern);
    }

    /**
     * 使用Redis的消息队列
     *
     * @param channel
     * @param message 消息内容
     */
    public void convertAndSend(String channel, Object message) {
        redisTemplate.convertAndSend(channel, message);
    }


    //=========BoundListOperations 用法 start============

    public void lRightPushAll(String key, long timeout, TimeUnit timeUnit, String... values) {
        //绑定操作
        BoundListOperations<String, String> boundValueOperations = redisTemplate.boundListOps( key);
        //插入数据
        boundValueOperations.rightPushAll(values);
        //设置过期时间
        boundValueOperations.expire(timeout, timeUnit);
    }

    public void lRightPush(String key, long timeout, TimeUnit timeUnit, String value) {
        //绑定操作
        BoundListOperations<String, String> boundValueOperations = redisTemplate.boundListOps( key);
        //插入数据
        boundValueOperations.rightPush(value);
        //设置过期时间
        boundValueOperations.expire(timeout, timeUnit);
    }

    public String lRightPop(String key) {
        //绑定操作
        BoundListOperations<String, String> boundValueOperations = redisTemplate.boundListOps( key);
        return boundValueOperations.rightPop();
    }

    public void lLeftPushALL(String key, long timeout, TimeUnit timeUnit, String... values) {
        //绑定操作
        BoundListOperations<String, String> boundValueOperations = redisTemplate.boundListOps( key);
        //插入数据
        boundValueOperations.leftPushAll(values);
        //设置过期时间
        boundValueOperations.expire(timeout, timeUnit);
    }

    public void lLeftPush(String key, long timeout, TimeUnit timeUnit, String value) {
        //绑定操作
        BoundListOperations<String, String> boundValueOperations = redisTemplate.boundListOps( key);
        //插入数据
        boundValueOperations.leftPush(value);
        //设置过期时间
        boundValueOperations.expire(timeout, timeUnit);
    }

    public String lLeftPop(String key) {
        //绑定操作
        BoundListOperations<String, String> boundValueOperations = redisTemplate.boundListOps( key);
        return boundValueOperations.leftPop();
    }

    public List<String> lRang(String key, long start, long end) {
        //绑定操作
        BoundListOperations<String, String> boundValueOperations = redisTemplate.boundListOps( key);
        //查询数据
        return boundValueOperations.range(start, end);
    }

    //=========ZSetOperations 用法 start============


    public Long zsetAdd(String key, Set<ZSetOperations.TypedTuple<String>> tuples) {
        try {
            return redisTemplate.opsForZSet().add( key, tuples);
        } catch (Exception e) {
        }
        return null;
    }

    /**
     * 获取有序集合大小
     *
     * @param key
     * @return
     */
    public Long zsetZCard(String key) {
        try {
            return redisTemplate.opsForZSet().zCard( key);
        } catch (Exception e) {
        }
        return null;
    }


    /**
     * 使用value有序集合增加元素的分数increment
     *
     * @param key
     * @param value
     * @param delta
     * @return
     */
    public Double zsetIncr(String key, String value, long delta) {
        try {
            return redisTemplate.opsForZSet().incrementScore( key, value, delta);
        } catch (Exception e) {
        }
        return 0.0;
    }
    /**
     * 使用value有序集合增加元素的分数increment
     *
     * @param key
     * @param value
     * @param delta
     * @return
     */
    public Double zsetIncr(String key, String value, double delta) {
        try {
            return redisTemplate.opsForZSet().incrementScore( key, value, delta);
        } catch (Exception e) {
        }
        return 0.0;
    }


    /**
     * values从排序集中删除。
     *
     * @param key
     * @param value
     * @return
     */
    public Long zsetRemove(String key, String value) {
        try {
            return redisTemplate.opsForZSet().remove( key, value);
        } catch (Exception e) {
        }
        return null;
    }

    /**
     * 获取范围的元素来自start于end从下令从高分到低分排序集。
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Set<String> zsetReverseRange(String key, long start, long end) {
        try {
            return redisTemplate.opsForZSet().reverseRange( key, start, end);
        } catch (Exception e) {
        }
        return null;
    }

    /**
     * 获取范围的元素来自start于end从下令从高分到低分排序集 以及分数
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Set<ZSetOperations.TypedTuple<String>> zsetReverseRangeWithScores(String key, long start, long end) {
        try {
            return redisTemplate.opsForZSet().reverseRangeWithScores( key, start, end);
        } catch (Exception e) {
        }
        return null;
    }

    /**
     * 获取绑定键中的元素的下标 正序
     *
     * @param key
     * @param value
     * @return
     */
    public Long zsetRank(String key, String value) {
        try {
            return redisTemplate.opsForZSet().rank( key, value);
        } catch (Exception e) {
        }
        return null;
    }

    /**
     * 获取绑定键中的元素的下标 倒序
     *
     * @param key
     * @param value
     * @return
     */
    public Long zsetReverseRank(String key, String value) {
        try {
            return redisTemplate.opsForZSet().reverseRank( key, value);
        } catch (Exception e) {
        }
        return null;
    }

    public Double zScore(String key, String value) {
        try {
            return redisTemplate.opsForZSet().score( key, value);
        } catch (Exception e) {
        }
        return null;
    }

    public Boolean zAdd(String key, String value, long score) {
        BoundZSetOperations<String, String> boundZSetOperations = redisTemplate.boundZSetOps( key);
        return boundZSetOperations.add(value, score);
    }

    public Long zAdd(String key, Set<ZSetOperations.TypedTuple<String>> values) {
        BoundZSetOperations<String, String> boundZSetOperations = redisTemplate.boundZSetOps( key);
        return boundZSetOperations.add(values);
    }

    public Long zRem(String key, String ... value) {
        BoundZSetOperations<String, String> boundZSetOperations = redisTemplate.boundZSetOps( key);
        return boundZSetOperations.remove(value);
    }

    public Long zRemRangeByScore(String key, double start, double end) {
        if(end < start) {
            return 0L;
        }
        BoundZSetOperations<String, String> boundZSetOperations = redisTemplate.boundZSetOps( key);
        return boundZSetOperations.removeRangeByScore(start, end);
    }

    public Set<ZSetOperations.TypedTuple<String>> zRangeWithScore(String key, long start, long stop) {
        BoundZSetOperations<String, String> boundZSetOperations = redisTemplate.boundZSetOps( key);
        return boundZSetOperations.rangeWithScores(start, stop);
    }

    public Set<ZSetOperations.TypedTuple<String>> zRangeByScore(String key, double start, double end) {
        BoundZSetOperations<String, String> boundZSetOperations = redisTemplate.boundZSetOps( key);
        return boundZSetOperations.rangeByScoreWithScores(start, end);
    }

    public Set<String> zRange(String key, long start, long stop) {
        BoundZSetOperations<String, String> boundZSetOperations = redisTemplate.boundZSetOps( key);
        return boundZSetOperations.range(start, stop);
    }

    public Long zRank(String key, String value) {
        BoundZSetOperations<String, String> boundZSetOperations = redisTemplate.boundZSetOps( key);
        return boundZSetOperations.rank(value);
    }

    public Long zReverse(String key) {
        BoundZSetOperations<String, String> boundZSetOperations = redisTemplate.boundZSetOps( key);
        return boundZSetOperations.reverseRank(key);
    }

    public Long zCard(String key) {
        BoundZSetOperations<String, String> boundZSetOperations = redisTemplate.boundZSetOps( key);
        return boundZSetOperations.zCard();
    }

    public Cursor<Map.Entry<Object, Object>> hScan(String key, String pattern, long count) {
        return redisTemplate.opsForHash().scan(key, ScanOptions.scanOptions().match(pattern).count(count).build());
    }

    public Cursor<ZSetOperations.TypedTuple<String>> zScan(String key, String pattern, long count) {
        return redisTemplate.opsForZSet().scan(key, ScanOptions.scanOptions().match(pattern).count(count).build());
    }

    /**
     * 通过分数返回有序集合指定区间内的成员个数
     * @return
     */
    public Long zCount(String key,Double start,Double end){
        return redisTemplate.opsForZSet().count(key,start,end);
    }

    /**
     * 获取范围的元素来自start于end分数排序集
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Set<ZSetOperations.TypedTuple<String>> zsetReverseRangeByScoreWithScores(String key, Double start, Double end) {
        try {
            return redisTemplate.opsForZSet().reverseRangeByScoreWithScores( key, start, end);
        } catch (Exception e) {
        }
        return null;
    }

    /**
     * 通过管道批量获取
     * @return
     */
    public List<Object> pipelined(Collection<String> keys){
        List<Object> list=redisTemplate.executePipelined(new RedisCallback<Long>() {
            @Nullable
            @Override
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                connection.openPipeline();
                for (String key : keys) {
                    connection.get(( key).getBytes());
                }
                return null;
            }
        });
        return list;
    }

    /**
     * 通过管道批量获取
     * @return
     */
    public List<String> mutiGet(Collection<String> keys){
        List<String> list=new ArrayList<>();
        keys.forEach(x->{
            list.add(x);
        });
        List<String> list1 = redisTemplate.opsForValue().multiGet(list);
        return list1;
    }
    /**
     * 通过管道批量保存
     * @return
     */
    public void mutiSet(Map<String, String> valueMap){
        Map<String, String> newValueMap = new HashMap<>();
        valueMap.forEach((key, value) -> {
            newValueMap.put(key, value);
        });
        redisTemplate.opsForValue().multiSet(newValueMap);
    }
}
