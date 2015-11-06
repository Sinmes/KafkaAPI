package zork.agent.module.consumercore.baseconsumers.baselowlevelavroconsumer;

import org.apache.avro.generic.GenericRecord;
import zork.agent.module.consumercore.baseanalytical.BaseAvroAnalytical;
import zork.agent.module.consumercore.model.IncomingDataPoint;
import zork.agent.module.consumercore.utils.optsdbutil.TimeSeriesStorer;
import zork.agent.module.consumercore.utils.zkutil.CuratorFrameworkZkUtils;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Sinme on 2015/11/3.
 */
public class BaseOptsFetchController extends BaseAvroFetchController{
    public BaseAvroAnalytical baseAvroAnalytical;
    public String url;
    public TimeSeriesStorer timeSeriesStorer;

    public BaseOptsFetchController(int partitionNum, String url, String scheama, CuratorFrameworkZkUtils zkUtils, BaseAvroAnalytical baseAvroAnalytical) {
        super(partitionNum, scheama, zkUtils);
        this.baseAvroAnalytical = baseAvroAnalytical;
        this.url = url;
        this.timeSeriesStorer = new TimeSeriesStorer();
    }

    @Override
    public void exec(long a_maxReads, String a_topic, String group, List<String> a_seedBrokers, int a_port) throws Exception {
        zkUtils.getOrmakeOffsetsPath(a_topic+group, this.partitionNum);
        for (int a_partition = 0; a_partition < this.partitionNum; a_partition++) {
            scheduExec.scheduleAtFixedRate(
                    new BaseOptsFetchThread(this, a_maxReads, a_topic, group, a_partition, a_seedBrokers, a_port), 0, 1000,
                    TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public Object HandleOnedata(GenericRecord genericRecord) {
        List<IncomingDataPoint> dataPoints = (List<IncomingDataPoint>) this.baseAvroAnalytical.Parse(this.keys, genericRecord);
        return dataPoints;
    }

    @Override
    public boolean HandleAlldata(Object dataPoints) {
        return this.timeSeriesStorer.storeTimePoints(this.url, (List<IncomingDataPoint>)dataPoints);
    }
}
