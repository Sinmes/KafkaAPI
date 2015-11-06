package zork.agent.module.consumercore.baseconsumers.baselowlevelavroconsumer;

import org.apache.avro.generic.GenericRecord;
import zork.agent.module.consumercore.baseanalytical.BaseAvroAnalytical;
import zork.agent.module.consumercore.utils.zkutil.CuratorFrameworkZkUtils;
import zork.agent.module.datasource.redisservice.Monitor;
import zork.agent.module.datasource.redisservice.RedisService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by Sinme on 2015/11/3.
 */
public class BaseRedisFetchController extends BaseAvroFetchController{
    public BaseAvroAnalytical baseAvroAnalytical;
    public RedisService redisService;

    public BaseRedisFetchController(int partitionNum, String scheama, CuratorFrameworkZkUtils zkUtils, BaseAvroAnalytical baseAvroAnalytical) {
        super(partitionNum, scheama, zkUtils);
        this.baseAvroAnalytical = baseAvroAnalytical;
        this.redisService = new RedisService();
    }

    @Override
    public Object HandleOnedata(GenericRecord genericRecord) {
        List<Monitor> list = (List<Monitor>) this.baseAvroAnalytical.Parse(this.keys, genericRecord);
        return list;
    }

    @Override
    public boolean HandleAlldata(Object lists) {
        return this.redisService.mSetRedis((List<Monitor>)lists);
    }

    @Override
    public void exec(long a_maxReads, String a_topic,String group, List<String> a_seedBrokers, int a_port) throws Exception {
        zkUtils.getOrmakeOffsetsPath(a_topic+group, this.partitionNum);
        for (int a_partition = 0; a_partition < this.partitionNum; a_partition++) {
            scheduExec.scheduleAtFixedRate(
                    new BaseRedisFetchThread(this, a_maxReads, a_topic, group, a_partition, a_seedBrokers, a_port), 0, 1000,
                    TimeUnit.MILLISECONDS);
        }
    }
}
