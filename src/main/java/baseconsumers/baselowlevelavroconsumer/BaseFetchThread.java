package zork.agent.module.consumercore.baseconsumers.baselowlevelavroconsumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import zork.agent.utility.LogHelper;

public class BaseFetchThread implements Runnable {
    public BaseFetchController FetchController;
    public List<String> m_replicaBrokers;
    public long a_maxReads;
    public String a_topic;
    public String group;
    public int a_partition;
    public List<String> a_seedBrokers;
    public int a_port;
    public long readOffset;
    
    public BaseFetchThread(BaseFetchController baseFetchController, long a_maxReads, String a_topic, String group, int a_partition, List<String> a_seedBrokers, int a_port) {
        this.FetchController = baseFetchController;
        this.m_replicaBrokers = new ArrayList<String>();
        this.a_maxReads = a_maxReads;
        this.a_topic = a_topic;
        this.group = group;
        this.a_partition = a_partition;
        this.a_seedBrokers = a_seedBrokers;
        this.a_port = a_port;
        
    }
    
    private PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port, String a_topic, int a_partition) {
        PartitionMetadata returnMetaData = null;
        loop:
        for (String seed : a_seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024, "leaderLookup");
                List<String> topics = Collections.singletonList(a_topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == a_partition) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
            	String st = ("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic
                        + ", " + a_partition + "] Reason: " + e);
            	LogHelper.info(String.format("error：%s", st), true);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (returnMetaData != null) {
            m_replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                m_replicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }
    
    private String findNewLeader(String a_oldLeader, String a_topic, int a_partition, int a_port) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port, a_topic, a_partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                	LogHelper.info(String.format("error：%s", "findNewLeader"+ie), true);
                }
            }
        }
    	LogHelper.info(String.format("error：%s", "Unable to find new leader after Broker failure. Exiting"), true);
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }
    
	@Override
	public void run() {
        this.init();
        // find the meta data about the topic and partition we are interested in
        PartitionMetadata metadata = findLeader(a_seedBrokers, a_port, a_topic, a_partition);
        if (metadata == null) {

            LogHelper.info(String.format("error ：%s", "Can't find metadata for Topic and Partition. Exiting"), true);
            return;
        }
        if (metadata.leader() == null) {
            LogHelper.info(String.format("error ：%s", "Can't find metadata for Topic and Partition. Exiting"), true);
            return;
        }
        String leadBroker = metadata.leader().host();
        String clientName = "Client_" + a_topic + "_" + a_partition;
        SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName);
        readOffset = FetchController.zkUtils.getOffset(a_topic+group, a_partition);
        long beginOffset = readOffset;
        int numErrors = 0;
        boolean flag = true;
        while (flag) {
            if (consumer == null) {
                consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName);
            }
            FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(a_topic, a_partition, readOffset, 100000) // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
                    .build();
            FetchResponse fetchResponse = consumer.fetch(req);
            if (fetchResponse.hasError()) {
                numErrors++;
                short code = fetchResponse.errorCode(a_topic, a_partition);
                LogHelper.info(String.format("error ：%s", "Error fetching data from the Broker:" + leadBroker + " Reason: " + code), true);
                if (numErrors > 5) break;
                if (code == ErrorMapping.OffsetOutOfRangeCode())  {
                    readOffset = FetchController.zkUtils.getOffset(a_topic+group, a_partition);
                    continue;
                }
                consumer.close();
                consumer = null;
                try {
                    leadBroker = findNewLeader(leadBroker, a_topic, a_partition, a_port);
                } catch (Exception e) {
                	LogHelper.info(String.format("error：%s", e), true);
                }
                continue;
            }
            numErrors = 0;
            long numRead = 0;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(a_topic, a_partition)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset) {
                	LogHelper.info(String.format("warn ：%s", "Found an old offset: " + currentOffset + " Expecting: " + readOffset), true);
                    continue;
                }
                readOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                try {
                    //在这里处理每一条数据！
                    doOne(bytes);
                } catch (Exception e) {
                    FetchController.zkUtils.commitOffset(a_topic+group, a_partition, beginOffset);
                    e.printStackTrace();
                    LogHelper.info(String.format("error in fetchthread,line is ：%s", new String(bytes)), true);
                }
                numRead++;
                if(numRead>=a_maxReads){
                	flag = false;
                	break;
                }
            }
            if (numRead == 0) {
                try { 
                	LogHelper.info(String.format("warn ：%s",Thread.currentThread().getId()+"is waiting data!"+beginOffset), true);
                	Thread.sleep(1000);
                } catch (InterruptedException ie) {
                	LogHelper.info(String.format("error ：%s", "no more data, sleep "+ie), true);
                }
            }
        }
        //在这里处理此线程的全部数据！
        doAll();
        if (consumer != null) consumer.close();
	}

    public void init(){}

    public void doOne(byte[] bytes){}

    public void doAll(){}
}
