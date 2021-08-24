package com.whaleal.xxl.handler;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import org.springframework.stereotype.Component;

@Component
public class HelloHandler {
    @XxlJob("helloHandler")
    public ReturnT<String> demoJobHandler(String param) throws Exception {
        System.out.println("获取到的参加" + param);
        System.out.println("Hello,World");
        //XxlJobLogger.log("XXL-JOB, Hello World.");
        return ReturnT.SUCCESS;
    }
}
