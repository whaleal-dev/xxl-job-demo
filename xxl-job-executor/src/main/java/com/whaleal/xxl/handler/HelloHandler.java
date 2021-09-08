package com.whaleal.xxl.handler;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;

@Component
public class HelloHandler {

    @Autowired
    private ThreadPoolExecutor executor;

    @XxlJob("helloHandler")
    public ReturnT<String> demoJobHandler() {

        //异步执行这个任务
        CompletableFuture.runAsync(() -> {
            System.out.println("开始执行异步任务");
            String jobParam = XxlJobHelper.getJobParam();
            System.out.println("XxlJobHelper中的参数" + jobParam);
            try {
                System.out.println("开始休眠");
                Thread.sleep(50000L);
                System.out.println("休眠结束");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, executor);
        //XxlJobLogger.log("XXL-JOB, Hello World.");
        return ReturnT.SUCCESS;
    }
}
