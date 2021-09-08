package com.whaleal.xxl.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
public class MyThreadConfig {

    @Bean
    public ThreadPoolExecutor threadPoolExecutor(ThreadPoolConfigProperties threadPoolConfigProperties) {

        /*
        * 根据配置文件来生成一个线程池，并确定线程池里面的工作情况
        * 具体的参数有：
        *   线程池一直存活的核心线程数量
        *   线程池可以拥有的最大的线程数量
        *   当前线程池的线程数量大于核心线程数量的时候，多余的线程数要多久被回收
        *   回收的单位
        *   任务过多时，存储任务的阻塞队列
        *   线程的创建工厂，这个使用的就是默认的
        *   线程池任务满载后采取的任务拒绝策略
        * */
        return new ThreadPoolExecutor(
                threadPoolConfigProperties.getCoreSize(),
                threadPoolConfigProperties.getMaxSize(),
                threadPoolConfigProperties.getKeepAliveTime(),
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(10000),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy()
        );
    }
}
