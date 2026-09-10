package com.xcare.order.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Redisson Distributed Lock & Redis 7.2 Configuration.
 */
@Configuration
public class RedissonConfig {

    @Value("${spring.data.redis.host:localhost}")
    private String redisHost;

    @Value("${spring.data.redis.port:6379}")
    private int redisPort;

    @Value("${spring.data.redis.password:}")
    private String redisPassword;

    @Value("${spring.data.redis.timeout:5000}")
    private int timeout;

    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient() {
        Config config = new Config();
        config.setCodec(new JsonJacksonCodec());

        String address = String.format("redis://%s:%d", redisHost, redisPort);
        var singleServer = config.useSingleServer()
                .setAddress(address)
                .setTimeout(timeout)
                .setConnectTimeout(timeout)
                .setConnectionMinimumIdleSize(8)
                .setConnectionPoolSize(32)
                .setRetryAttempts(3)
                .setRetryInterval(1500);

        if (redisPassword != null && !redisPassword.isBlank()) {
            singleServer.setPassword(redisPassword);
        }

        return Redisson.create(config);
    }
}
