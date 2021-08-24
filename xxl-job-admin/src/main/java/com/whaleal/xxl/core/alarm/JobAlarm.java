package com.whaleal.xxl.core.alarm;

import com.whaleal.xxl.core.model.XxlJobInfo;
import com.whaleal.xxl.core.model.XxlJobLog;

/**
 * @author xuxueli 2020-01-19
 */
public interface JobAlarm {

    /**
     * job alarm
     *
     * @param info
     * @param jobLog
     * @return
     */
    public boolean doAlarm(XxlJobInfo info, XxlJobLog jobLog);

}
