package com.whaleal.xxl.handler;

import cache.MemoryCache;
import com.whaleal.photon.source.oracle.execute.OracleSource;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import common.OplogMetadata;
import common.dbtype.DbTypeFlag;
import common.photonV.entity.Datasource;
import common.photonV.entity.ProgramInfo;
import common.photonV.entity.TaskTrigger;
import common.taskbase.AbstractTargetTask;
import common.taskbase.metadata.SourceMetadata;
import datasource.DataSourceUtil;
import dbconnection.mongodb.MongoDbConnection;
import dbconnection.mysql.MySqlConnection;
import dbconnection.oracle.OracleConnection;
import dbconnection.pgserver.PgServerConnection;
import execute.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import programInfo.ProgramInfoUtil;
import task.OplogNsBucketTask;
import task.OplogNsTask;
import task.OplogReadTask;
import task.OplogWriteTask;
import thread.SourceTaskPoolManager;
import thread.SysPoolManager;
import thread.TargetTaskPoolManager;
import trigger.TriggerUtil;
import util.Log;
import util.StringUtil;

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
                Thread.sleep(10000);
                long batchNo = System.currentTimeMillis();
                exe("proc2", batchNo);
                System.out.println("休眠结束");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, executor);
        //XxlJobLogger.log("XXL-JOB, Hello World.");
        return ReturnT.SUCCESS;
    }

    public void exe(String procName, long batchNo) throws InterruptedException {
        String procNameAndBatchNo = procName + batchNo;
        //创建pro
        ProgramInfo programInfo = ProgramInfoUtil.getProgramInfo(procName);
        if (programInfo == null || programInfo.getProName().length() == 0) {
            return;
        }
        programInfo.setBatchNO(batchNo);
        //获取数据源对象
        Datasource dataSourceDb = DataSourceUtil.getDataSourceByDsName(programInfo.getSourceDsName());
        Datasource dataTargetDb = DataSourceUtil.getDataSourceByDsName(programInfo.getTargetDsName());
        //创建数据源链接
        createDataSourceConnection(procNameAndBatchNo, dataSourceDb, dataTargetDb);
        //创建触发对象实例
        TaskTrigger taskTrigger = generateTaskTriggerInfo(programInfo.getTaskName(), programInfo.getProName());
        TriggerUtil.insertTriggerInfo(taskTrigger);
        //创建线程池
        createThreadPoolManager(programInfo);
        //创建缓存类
        MemoryCache memoryCache = new MemoryCache(programInfo.getTaskName(), programInfo.getProName(), programInfo.getCacheNum(), programInfo.getCacheSize(), true);
        programInfo.setMemoryCache(memoryCache);
        // 开始传输数据
        generateTranT(dataSourceDb, dataTargetDb, programInfo, taskTrigger, memoryCache);
        disDataSourceConnection(procNameAndBatchNo, dataSourceDb, dataTargetDb);
        destroy(procName, batchNo, memoryCache);
    }

    public void generateTranT(Datasource dataSourceDb, Datasource dataTargetDb, ProgramInfo programInfo, TaskTrigger taskTrigger, MemoryCache memoryCache) {
        if (dataSourceDb.getType().equalsIgnoreCase(DbTypeFlag.MONGODB) && dataTargetDb.getType().equalsIgnoreCase(DbTypeFlag.MONGODB)) {
            testMongoDbToMongoDb(programInfo, memoryCache, taskTrigger);
        } else if (dataSourceDb.getType().equalsIgnoreCase(DbTypeFlag.MYSQL) && dataTargetDb.getType().equalsIgnoreCase(DbTypeFlag.MONGODB)) {
            testMysqlToMongoDb(programInfo, memoryCache, taskTrigger);
        } else if (dataSourceDb.getType().equalsIgnoreCase(DbTypeFlag.PG) && dataTargetDb.getType().equalsIgnoreCase(DbTypeFlag.MONGODB)) {
            testPgToMongoDb(programInfo, memoryCache, taskTrigger);
        } else if (dataSourceDb.getType().equalsIgnoreCase(DbTypeFlag.ORACLE) && dataTargetDb.getType().equalsIgnoreCase(DbTypeFlag.MONGODB)) {
            testOracleToMongoDb(programInfo, memoryCache, taskTrigger);
        } else if (dataSourceDb.getType().equalsIgnoreCase(DbTypeFlag.ORACLE) && dataTargetDb.getType().equalsIgnoreCase(DbTypeFlag.MYSQL)) {
            testOracleToMysql(programInfo, memoryCache, taskTrigger);
        } else if (dataSourceDb.getType().equalsIgnoreCase(DbTypeFlag.MONGODB) && dataTargetDb.getType().equalsIgnoreCase(DbTypeFlag.MYSQL)) {
            testMongoDbToMysql(programInfo, memoryCache, taskTrigger);
        } else if (dataSourceDb.getType().equalsIgnoreCase(DbTypeFlag.PG) && dataTargetDb.getType().equalsIgnoreCase(DbTypeFlag.MYSQL)) {
            testPgToMysql(programInfo, memoryCache, taskTrigger);
        } else if (dataSourceDb.getType().equalsIgnoreCase(DbTypeFlag.MYSQL) && dataTargetDb.getType().equalsIgnoreCase(DbTypeFlag.MYSQL)) {
            testMysqlToMysql(programInfo, memoryCache, taskTrigger);
        }
    }

    public void createDataSourceConnection(String procNameAndBatchNo, Datasource... dataSourceList) {

        for (Datasource dataSourceDb : dataSourceList) {
            String procNameAndBatchNoAndDsName = procNameAndBatchNo + dataSourceDb.getName();
            try {
                if (dataSourceDb.getType().equalsIgnoreCase(DbTypeFlag.MONGODB)) {
                    MongoDbConnection.createMonoDbClient(procNameAndBatchNoAndDsName, dataSourceDb);
                } else if (dataSourceDb.getType().equalsIgnoreCase(DbTypeFlag.MYSQL)) {
                    MySqlConnection.createConnection(procNameAndBatchNoAndDsName, dataSourceDb);
                } else if (dataSourceDb.getType().equalsIgnoreCase(DbTypeFlag.PG)) {
                    PgServerConnection.createConnection(procNameAndBatchNoAndDsName, dataSourceDb);
                } else if (dataSourceDb.getType().equalsIgnoreCase(DbTypeFlag.ORACLE)) {
                    OracleConnection.createConnection(procNameAndBatchNoAndDsName, dataSourceDb);
                }
            } catch (Exception e) {
                e.printStackTrace();
                Log.error(e.getMessage());
            }
        }
    }

    public void createThreadPoolManager(ProgramInfo programInfo) {
        String procNameAndBatchNo = programInfo.getProName() + programInfo.getBatchNO();
        SourceTaskPoolManager sourceTaskPoolManager = new SourceTaskPoolManager(procNameAndBatchNo, programInfo.getSourceThreadNum(), programInfo.getSourceThreadNum());

        SysPoolManager sysPoolManager = new SysPoolManager(procNameAndBatchNo, 2, 3);

        TargetTaskPoolManager targetTaskPoolManager = new TargetTaskPoolManager(procNameAndBatchNo, programInfo.getTargetThreadNum(), programInfo.getTargetThreadNum());

    }

    public void getProExeInfo(ProgramInfo programInfo, SourceMetadata sourceMetadata, MemoryCache memoryCache) {
        String procNameAndBatchNo = programInfo.getProName() + programInfo.getBatchNO();
        while (true) {
            try {
                Thread.sleep(10000);
                int sourceThread = SourceTaskPoolManager.setSourceActiveThreadNum(procNameAndBatchNo, 0);
                boolean getAllDbTable = sourceMetadata.isGetAllDbTable();
                int sourceTaskQueueSize = sourceMetadata.getTaskMetadataQueueSize();
                int setSysActiveThreadNum = SysPoolManager.setSysActiveThreadNum(procNameAndBatchNo, 0);
                int targetActiveThreadNum = TargetTaskPoolManager.setTargetActiveThreadNum(procNameAndBatchNo, 0);
                int allDataCacheNum = memoryCache.getAllDataCacheNum();
                Log.info(setSysActiveThreadNum + "");
                Log.info("sum:" + (sourceThread + sourceTaskQueueSize + allDataCacheNum + setSysActiveThreadNum));
                Log.info("sourceThread:" + sourceThread);
                Log.info("sourceTaskQueueSize:" + sourceTaskQueueSize);
                Log.info("setSysActiveThreadNum:" + setSysActiveThreadNum);
                Log.info("targetActiveThreadNum:" + targetActiveThreadNum);
                Log.info("allDataCacheNum:" + allDataCacheNum);
                Log.info("getAllDbTable:" + getAllDbTable);
                if ((sourceThread + sourceTaskQueueSize + allDataCacheNum + setSysActiveThreadNum + targetActiveThreadNum) == 0 && getAllDbTable) {
                    Thread.sleep(10000);
                    break;
                } else if ((sourceThread + sourceTaskQueueSize + sourceTaskQueueSize + allDataCacheNum + setSysActiveThreadNum) == 0 && getAllDbTable) {
                    AbstractTargetTask.setIsStopFlagOfTarget(procNameAndBatchNo, true);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void destroy(String procName, long batchNo, MemoryCache memoryCache) {
        String procNameAndBatchNo = procName + batchNo;

        TargetTaskPoolManager.destroy(procNameAndBatchNo);
        SourceTaskPoolManager.destroy(procNameAndBatchNo);
        SysPoolManager.destroy(procNameAndBatchNo);

        AbstractTargetTask.removeIsStopFlagOfTarget(procNameAndBatchNo);

        memoryCache.gcMemoryCache();
        Log.error("procName:" + procNameAndBatchNo + "关闭成功");
    }

    public void disDataSourceConnection(String procNameAndBatchNo, Datasource... dataSourceList) {
        for (Datasource dataSourceDb : dataSourceList) {
            String procNameAndBatchNoAndDsName = procNameAndBatchNo + dataSourceDb.getName();
            try {
                if (dataSourceDb.getType().equalsIgnoreCase(DbTypeFlag.MONGODB)) {
                    MongoDbConnection.close(procNameAndBatchNoAndDsName);
                } else if (dataSourceDb.getType().equalsIgnoreCase(DbTypeFlag.MYSQL)) {
                    MySqlConnection.close(procNameAndBatchNoAndDsName);
                } else if (dataSourceDb.getType().equalsIgnoreCase(DbTypeFlag.PG)) {
                    PgServerConnection.close(procNameAndBatchNoAndDsName);
                } else if (dataSourceDb.getType().equalsIgnoreCase(DbTypeFlag.ORACLE)) {
                    OracleConnection.close(procNameAndBatchNoAndDsName);
                }
            } catch (Exception e) {
                e.printStackTrace();
                Log.error(e.getMessage());
            }
        }
    }

    public void testRealTimeOfMongodb() {
        ProgramInfo programInfo = ProgramInfoUtil.getProgramInfo("proc3");
        programInfo.setDbTableWhite("\\w.+");
        MongoDbConnection.createMonoDbClient(programInfo.getSourceDsName(), DataSourceUtil.getDataSourceByDsName(programInfo.getSourceDsName()));

        MongoDbConnection.createMonoDbClient(programInfo.getTargetDsName(), DataSourceUtil.getDataSourceByDsName(programInfo.getTargetDsName()));


        SourceTaskPoolManager sourceTaskPoolManager = new SourceTaskPoolManager(programInfo.getProName(), programInfo.getSourceThreadNum(), programInfo.getSourceThreadNum());


        TargetTaskPoolManager targetTaskPoolManager = new
                TargetTaskPoolManager(programInfo.getProName(), 10,
                10);

        OplogMetadata oplogMetadata = new OplogMetadata(programInfo);

        TargetTaskPoolManager.submit(programInfo.getProName(), new OplogWriteTask(oplogMetadata));
        TargetTaskPoolManager.submit(programInfo.getProName(), new OplogNsBucketTask(oplogMetadata, programInfo.getCacheSize()));
        TargetTaskPoolManager.submit(programInfo.getProName(), new OplogNsTask(programInfo.getDbTableWhite(), oplogMetadata));
        TargetTaskPoolManager.submit(programInfo.getProName(), new OplogReadTask(programInfo, (int) (System.currentTimeMillis() / 1000)
                , 0, 0, oplogMetadata));
        for (int i = 0; i < 2; i++) {
            TargetTaskPoolManager.submit(programInfo.getProName(), new OplogWriteTask(oplogMetadata));
            TargetTaskPoolManager.submit(programInfo.getProName(), new OplogNsBucketTask(oplogMetadata, programInfo.getCacheSize()));
        }

    }

    public void testMongoDbToMysql(ProgramInfo programInfo, MemoryCache memoryCache, TaskTrigger taskTrigger) {


        MongodbSource mongodbSource = new MongodbSource(programInfo, memoryCache);
        mongodbSource.createTask();

        MysqlTarget mysqlTarget = new MysqlTarget(programInfo, memoryCache, programInfo.getProName());
        mysqlTarget.startToTarget();

        getProExeInfo(programInfo, mongodbSource, memoryCache);
    }

    public void testPgToMysql(ProgramInfo programInfo, MemoryCache memoryCache, TaskTrigger taskTrigger) {


        PgSource pgSource = new PgSource(programInfo, memoryCache);
        pgSource.createTask();

        MysqlTarget mysqlTarget = new MysqlTarget(programInfo, memoryCache, programInfo.getProName());
        mysqlTarget.startToTarget();

        getProExeInfo(programInfo, pgSource, memoryCache);
    }

    public TaskTrigger generateTaskTriggerInfo(String taskName, String procName) {
        TaskTrigger taskTrigger = new TaskTrigger();
        taskTrigger.setId(StringUtil.generateUUID());
        taskTrigger.setTaskName(taskName);
        taskTrigger.setProcName(procName);
        return taskTrigger;
    }

    public void testMongoDbToMongoDb(ProgramInfo programInfo, MemoryCache memoryCache, TaskTrigger taskTrigger) {


        MongodbTarget mongodbTarget = new MongodbTarget(programInfo, memoryCache, programInfo.getProName());
        mongodbTarget.startToTarget();

        MongodbSource mongodbSource = new MongodbSource(programInfo, memoryCache);
        mongodbSource.createTask();

        getProExeInfo(programInfo, mongodbSource, memoryCache);
    }

    public void testMysqlToMongoDb(ProgramInfo programInfo, MemoryCache memoryCache, TaskTrigger taskTrigger) {

        MongodbTarget mongodbTarget = new MongodbTarget(programInfo, memoryCache, programInfo.getProName());
        mongodbTarget.startToTarget();

        MysqlSource mysqlSource = new MysqlSource(programInfo, memoryCache);
        mysqlSource.createTask();

        getProExeInfo(programInfo, mysqlSource, memoryCache);
    }

    public void testPgToMongoDb(ProgramInfo programInfo, MemoryCache memoryCache, TaskTrigger taskTrigger) {


        MongodbTarget mongodbTarget = new MongodbTarget(programInfo, memoryCache, programInfo.getProName());
        mongodbTarget.startToTarget();

        PgSource pgSource = new PgSource(programInfo, memoryCache);
        pgSource.createTask();

        getProExeInfo(programInfo, pgSource, memoryCache);
    }

    public void testOracleToMongoDb(ProgramInfo programInfo, MemoryCache memoryCache, TaskTrigger taskTrigger) {


        MongodbTarget mongodbTarget = new MongodbTarget(programInfo, memoryCache, programInfo.getProName());
        mongodbTarget.startToTarget();

        OracleSource oracleSource = new OracleSource(programInfo, memoryCache);
        oracleSource.createTask();
        getProExeInfo(programInfo, oracleSource, memoryCache);
    }

    public void testMysqlToMysql(ProgramInfo programInfo, MemoryCache memoryCache, TaskTrigger taskTrigger) {


        MysqlTarget mysqlTarget = new MysqlTarget(programInfo, memoryCache, programInfo.getProName());
        mysqlTarget.startToTarget();

        MysqlSource mysqlSource = new MysqlSource(programInfo, memoryCache);
        mysqlSource.createTask();
        getProExeInfo(programInfo, mysqlSource, memoryCache);
    }

    public void testOracleToMysql(ProgramInfo programInfo, MemoryCache memoryCache, TaskTrigger taskTrigger) {


        MysqlTarget mysqlTarget = new MysqlTarget(programInfo, memoryCache, programInfo.getProName());
        mysqlTarget.startToTarget();

        OracleSource oracleSource = new OracleSource(programInfo, memoryCache);
        oracleSource.createTask();
        getProExeInfo(programInfo, oracleSource, memoryCache);
    }

}
