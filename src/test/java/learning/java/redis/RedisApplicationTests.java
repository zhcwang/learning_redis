package learning.java.redis;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.*;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RedisApplicationTests {

    @Autowired
    private StringRedisTemplate srt;

    @Test
    public void contextLoads() {
        System.out.println(srt.opsForValue().get("msg"));
    }

    @Test
    public void opsTest(){
        ZSetOperations<String, String> stringStringZSetOperations = srt.opsForZSet();
        HashOperations<String, Object, Object> stringObjectObjectHashOperations = srt.opsForHash();
        ValueOperations<String, String> stringStringValueOperations = srt.opsForValue();
        ClusterOperations<String, String> stringStringClusterOperations = srt.opsForCluster();
        HyperLogLogOperations<String, String> stringStringHyperLogLogOperations = srt.opsForHyperLogLog();
        ListOperations<String, String> stringStringListOperations = srt.opsForList();
        SetOperations<String, String> stringStringSetOperations = srt.opsForSet();
    }

    /**
     * INCR是原子性操作
     */
    @Test
    public void incrTest(){
        ValueOperations<String, String> stringOps = srt.opsForValue();
        stringOps.set("multi:counter", "0");
        Runnable r = () -> stringOps.increment("multi:counter");
        for (int i = 0; i < 10000; i++) {
            new Thread(r).start();
        }
    }

    /**
     * 下面的例子模拟先获取再并发插入的情况，多线程竞争
     */
    @Test
    public void getAndIncr(){
        ValueOperations<String, String> stringOps = srt.opsForValue();
        stringOps.set("multi:counter", "0");
        Runnable r = () -> stringOps.set("multi:counter", String.valueOf(Integer.parseInt(stringOps.get("multi:counter") + 1)));
        for (int i = 0; i < 1000; i++) {
            new Thread(r).start();
        }
    }

    @Test
    public void safeGetAndIncr(){
        ValueOperations<String, String> stringOps = srt.opsForValue();
        stringOps.set("multi:counter", "0");
        for (int i = 0; i < 2; i++) {
            new Thread(() -> {
                String uuid = null;
                try {
                    uuid = acquireLock("task", 1000, 1000000);
                    if(StringUtils.isNotEmpty(uuid)){
                        int counter = Integer.parseInt(Objects.requireNonNull(stringOps.get("multi:counter")));
                        stringOps.set("multi:counter", String.valueOf(++counter));
                    } else {
                        System.out.println("获取锁失败，不执行任务。");
                    }
                } catch (Exception e){
                    // donothing
                } finally {
                    if(StringUtils.isNotEmpty(uuid)){
                        System.out.println("释放锁：" + uuid);
                        releaseLock("task", uuid);
                    }
                }
            }).start();
        }
    }

    /**
     * 获取分布式锁
     * 该锁主要通过定时释放而解决了多节点获取锁的问题
     * 但是假如对于网络处理，假如A节点获取了锁，开始执行，这时网络超时可能要卡很久，过期时间到，节点B获取了相同的锁
     * 这时候一个任务就会在两个节点执行了
     * @param key
     * @param holdTime
     * @param tryTimeOut
     * @return
     */
    private String acquireLock(String key, int holdTime, int tryTimeOut) throws Exception{
        ValueOperations<String, String> opsv = srt.opsForValue();
        String lockKey = "lock:" + key;
        long endTime = tryTimeOut + System.currentTimeMillis();
        String uuid = UUID.randomUUID().toString();
        while (System.currentTimeMillis() < endTime){
            try {
                Boolean success = opsv.setIfAbsent(lockKey, uuid);
                if (success != null && success){
                    srt.expire(lockKey, holdTime, TimeUnit.MILLISECONDS);
                    return uuid;
                } else {
                    // 防止其它节点获取锁后，没上过期时间
                    Long expire = srt.getExpire(lockKey);
                    if (expire.intValue() == -1){
                        srt.expire(lockKey, holdTime, TimeUnit.MILLISECONDS);
                    }
                }
                Thread.sleep(10);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private void releaseLock(String key, String uuid){
        ValueOperations<String, String> opsv = srt.opsForValue();
        String lockKey = "lock:" + key;
        if(StringUtils.equals(opsv.get(lockKey), uuid)){
            srt.delete(lockKey);
        } else {
            // 锁已经被其它线程获取了，不要删除该锁
        }

    }

}
