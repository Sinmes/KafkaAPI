package zork.agent.module.consumercore.baseconsumers.baselowlevelavroconsumer;

import zork.agent.module.consumercore.model.IncomingDataPoint;
import zork.agent.utility.LogHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Sinme on 2015/11/3.
 */
public class BaseOptsFetchThread extends BaseFetchThread{
    public List<IncomingDataPoint> dataPoints;
    public BaseOptsFetchController baseOptsFetchController;

    public BaseOptsFetchThread(BaseOptsFetchController baseOptsFetchController, long a_maxReads, String a_topic, String group, int a_partition, List<String> a_seedBrokers, int a_port) {
        super(baseOptsFetchController, a_maxReads, a_topic, group, a_partition, a_seedBrokers, a_port);
        this.baseOptsFetchController = baseOptsFetchController;
    }

    @Override
    synchronized public void init() {
        this.dataPoints = new ArrayList<IncomingDataPoint>();
    }

    @Override
    synchronized public void doOne(byte[] bytes) {
        dataPoints.addAll((List<IncomingDataPoint>) this.baseOptsFetchController.HandleOnedata(this.baseOptsFetchController.deserializing(bytes)));
    }

    @Override
    synchronized public void doAll() {
        if (this.baseOptsFetchController.HandleAlldata(dataPoints)){
            this.baseOptsFetchController.zkUtils.commitOffset(super.a_topic+group, super.a_partition, super.readOffset);
            LogHelper.info(String.format("info ：%s", Thread.currentThread().getId() + "commitOffset  : " + readOffset), true);
        }
    }
}
