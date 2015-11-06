package zork.agent.module.consumercore.baseconsumers.baselowlevelavroconsumer;

import zork.agent.module.datasource.redisservice.Monitor;
import zork.agent.module.datasource.redisservice.RedisService;
import zork.agent.utility.LogHelper;

import java.util.*;

/**
 * Created by Sinme on 2015/11/3.
 */
public class BaseRedisFetchThread extends BaseFetchThread{
    public List<Monitor> lists;
    public BaseRedisFetchController baseRedisFetchController;
    public RedisService redisService;

    public BaseRedisFetchThread(BaseRedisFetchController baseRedisFetchController, long a_maxReads, String a_topic, String group, int a_partition, List<String> a_seedBrokers, int a_port) {
        super(baseRedisFetchController, a_maxReads, a_topic, group, a_partition, a_seedBrokers, a_port);
        this.baseRedisFetchController = baseRedisFetchController;
    }

    @Override
    public void init() {
        this.lists = new ArrayList<Monitor>();
    }

    @Override
    public void doOne(byte[] bytes) {
        this.lists.addAll((List<Monitor>) this.baseRedisFetchController.HandleOnedata(this.baseRedisFetchController.deserializing(bytes)));
    }

    @Override
    public void doAll() {
        if (this.baseRedisFetchController.HandleAlldata(lists)){
            this.baseRedisFetchController.zkUtils.commitOffset(super.a_topic+group, super.a_partition, super.readOffset);
            LogHelper.info(String.format("info ：%s", Thread.currentThread().getId() + "commitOffset  : " + readOffset), true);
        }
    }
}
