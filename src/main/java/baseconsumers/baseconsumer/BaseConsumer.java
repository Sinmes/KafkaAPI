package zork.agent.module.consumercore.baseconsumers.baseconsumer;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import zork.agent.utility.LogHelper;

/**
 * 
 * @author Sin.Z
 *
 */
public class BaseConsumer extends Thread {
    /** 
    * @Fields body : TODO  消息体
    */ 
    private byte[] body;
    /** 
    * @Fields consumer : TODO  kafka consumer 的链接
    */ 
    private final ConsumerConnector consumer;
    /** 
    * @Fields topic : TODO  kafka的topic 
    */ 
    private final String topic;

   
    /** 
    * <p>Title: </p> 
    * <p>Description: </p> 构造函数
    * @param zk zookeeper地址
    * @param groupID Consumer所属groupID
    * @param topic 所要消费的topic。
    */
    public BaseConsumer(String zk, String groupID, String topic) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zk, groupID));
        this.topic = topic;
    }

   
    /** 
    * @Title: createConsumerConfig 
    * @Description: TODO kafka consumer 配置文件用于
    * @param zk zookeeper地址
    * @param groupID Consumer所属group
    * @return    设定文件 
    * ConsumerConfig    返回类型 
    * @throws 
    */
    private ConsumerConfig createConsumerConfig(String zk, String groupID) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zk);
        props.put("group.id", groupID);
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.interval.ms", "1000");
//        props.put("auto.offset.reset", "smallest");
        return new ConsumerConfig(props);
    }

   
    /** 
    * @Title: todo 用户继承此类后需要复写次方法，来对消息做定制的处理，如果不复写，则简单输出
    * @Description: TODO(这里用一句话描述这个方法的作用) 
    * @param body    设定文件 未经处理的kafka原始body
    * void    返回类型 
    * @throws 
    */
    public void todo(String  body){
        System.out.println(body);
    }

   
    /* (non Javadoc)
    * Title: run 
    * Description: 线程的主方法，用于从kafka获取消息，再调用todo()方法做处理，
    * @see java.lang.Thread#run()
    */
    @Override
    public void run() {
    	LogHelper.info(Thread.currentThread().getName() + " 初始化!", true);
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream =  consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while(it.hasNext()){
            body = it.next().message();
            todo(new String (body));
        }
    }
}
