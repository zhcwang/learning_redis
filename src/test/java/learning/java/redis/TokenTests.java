package learning.java.redis;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import javax.xml.bind.SchemaOutputResolver;
import java.util.Set;
import java.util.UUID;

@RunWith(SpringRunner.class)
@SpringBootTest
public class TokenTests {

    @Autowired
    private StringRedisTemplate srt;

    /**
     * 使用hash存储所有token:
     *  key : login:
     *  value : token -> userId
     * 使用zset保存所有用户的最近登录时间:
     *  key : recent:
     *  value : token -> timemillis
     */
    @Test
    public void contextLoads() {
        createRandomUserAndToken();
    }

    private void saveUserByToken(String token, String userId){
        srt.opsForHash().put("login:", token, userId);
    }

    private void refreshRecentTime(String token){
        srt.opsForZSet().add("recent:", token, System.currentTimeMillis());
    }

    private String getUserByToken(String token){
        return (String) srt.opsForHash().get("login:", token);
    }

    private void cleanToken(){
        // 升序获取所有在范围内的key
        Set<String> keys = srt.opsForZSet().range("zset-key", 0, 2);
        keys.forEach((key) -> {
            System.out.println(key);
        });
    }

    private void createRandomUserAndToken(){
        for (int i = 0; i < 100000; i++) {
            saveUserByToken(String.valueOf(i), UUID.randomUUID().toString());
            refreshRecentTime(String.valueOf(i));
        }
    }

}
