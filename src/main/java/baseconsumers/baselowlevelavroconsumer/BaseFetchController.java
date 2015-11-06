package zork.agent.module.consumercore.baseconsumers.baselowlevelavroconsumer;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericRecord;
import zork.agent.module.consumercore.baseanalytical.BaseAnalytical;
import zork.agent.module.consumercore.utils.zkutil.CuratorFrameworkZkUtils;

public class BaseFetchController {
	public ScheduledExecutorService scheduExec;
	public int partitionNum;
	public CuratorFrameworkZkUtils zkUtils;

	public BaseFetchController(int partitionNum, CuratorFrameworkZkUtils zkUtils) {
		this.partitionNum = partitionNum;
		this.scheduExec = Executors.newScheduledThreadPool(partitionNum);
		this.zkUtils = zkUtils;
	}

	public void exec(long a_maxReads, String a_topic, String group, List<String> a_seedBrokers, int a_port) throws Exception {
		zkUtils.getOrmakeOffsetsPath(a_topic+group, this.partitionNum);
		for (int a_partition = 0; a_partition < this.partitionNum; a_partition++) {
			scheduExec.scheduleAtFixedRate(
					new BaseFetchThread(this, a_maxReads, a_topic, group, a_partition, a_seedBrokers, a_port), 0, 1000,
					TimeUnit.MILLISECONDS);

		}
	}

	public void close() {
		scheduExec.shutdown();
	}

	public Object HandleOnedata(GenericRecord genericRecord) {
		return null;
	}

	public boolean HandleAlldata(Object dataPoints) {
		return true;
	}
}
