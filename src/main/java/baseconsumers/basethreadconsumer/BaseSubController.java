package zork.agent.module.consumercore.baseconsumers.basethreadconsumer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
/**
* @ClassName  : ConsumerController
* @Description  : TODO
* @author  :suyan
* @email  :suyan@zork.com.cn
*/
@SuppressWarnings("rawtypes") 
public class BaseSubController {
     /** 
    * @Fields connector : TODO  kakfa consumer 链接
    */ 
    private final ConsumerConnector connector;
     /** 
    * @Fields topic : TODO kafka topic 用于接收消息
    */ 
    private final String topic; 
     /** 
    * @Fields executor : TODO 线程池用户多个consumer 线程
    */ 
    private ExecutorService executor; 
     /** 
    * @Fields schema : TODO  用于序列化和反序列化的avro schema
    */ 
    private String schema;
     /** 
    * @Fields url : TODO  链接opentsdb 的url
    */ 
    private String url; 
     
     /** 
    * <p>Title: </p> 
    * <p>Description: </p>  ConsumerController构造函数
    * @param urlStirng 链接opentsdb 的url
    * @param a_zookeeper zookeeper 地址 ip:port
    * @param a_groupId consumer 接收组
    * @param a_topic kafka topic 用于消费消息
    * @param schema 用于序列化和反序列化的avro schema
    */
    public BaseSubController(String urlStirng, String a_zookeeper, String a_groupId, String a_topic, String schema) {
    	 connector = Consumer.createJavaConsumerConnector(
                 createConsumerConfig(a_zookeeper, a_groupId));
         this.topic = a_topic;
         url = urlStirng; 
         this.schema = schema;
     }
  
     /** 
    * @Title: shutdown 
    * @Description: TODO 停止consumer 和 线程池
    * void    返回类型 
    * @throws 
    */
    public void shutdown() {
         if (connector != null) connector.shutdown();
         if (executor != null) executor.shutdown();
     }
     
     /** 
    * @Title: run 
    * @Description: TODO 运行consumerControll 入口
    * @param a_numThreads   线程数
    * void    返回类型 
    * @throws 
    */
    public void run(int a_numThreads) {
//         Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
//         topicCountMap.put(topic, new Integer(a_numThreads));
//         Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = connector.createMessageStreams(topicCountMap);
//         List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
//         executor = Executors.newFixedThreadPool(a_numThreads);
//         int threadNumber = 0;
//         for (final KafkaStream stream : streams) {
//        	 executor.execute(new TestConsumer(url, stream, threadNumber, connector));
//        	 //executor.execute(new TsdAddConsumer(url, stream, threadNumber,connector, schema));
//             threadNumber++;
//         }
     }
    /** 
     * @Title: createConsumerConfig 
     * @Description: TODO consumer配置文件
     * @param zk zookeeper地址
     * @param groupID Consumer所属group
     * @return    设定文件 
     * ConsumerConfig    返回类型 
     * @throws 
     */
    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
         Properties props = new Properties();
         props.put("zookeeper.connect",a_zookeeper);
         props.put("group.id", a_groupId);
         props.put("zookeeper.connection.timeout.ms", "30000");
	     //props.put("rebalance.backoff.ms", "100000");
	     //props.put("zookeeper.session.timeout.ms", "100000");
	   	 props.put("zookeeper.sync.time.ms", "200");
	   	 props.put("auto.commit.interval.ms", "1000");
	   	 props.put("auto.offset.reset","largest");
	  
         return new ConsumerConfig(props);
     }
  
//     public static void main(String[] args) {
//         String zooKeeper = "10.176.3.102:2181,10.176.3.103:2181,10.176.3.104:2181";
//         String groupId = "ymg11";
//         String topic = "mykafka";
//         int threads = Integer.parseInt("3");
//         String json = ("{\n" +
//                 "    \"type\": \"record\",\n" +
//                 "    \"name\": \"yiyangzhi_hoststatus\",\n" +
//                 "    \"fields\": [\n" +
//                 "        {\n" +
//                 "            \"name\": \"host_name\",\n" +
//                 "            \"type\": [\"string\",\"null\"]\n" +
//                 "        },\n" +
//                 "        {\n" +
//                 "            \"name\": \"check_command\",\n" +
//                 "            \"type\": [\"string\",\"null\"]\n" +
//                 "        },\n" +
//                 "        {\n" +
//                 "            \"name\": \"performance_data\",\n" +
//                 "            \"type\": [\"string\",\"null\"]\n" +
//                 "        },\n" +
//                 "        {\n" +
//                 "            \"name\": \"plugin_output\",\n" +
//                 "            \"type\": [\"string\",\"null\"]\n" +
//                 "        },\n" +
//                 "        {\n" +
//                 "            \"name\": \"current_state\",\n" +
//                 "            \"type\": [\"string\",\"null\"]\n" +
//                 "        },\n" +
//                 "        {\n" +
//                 "            \"name\": \"last_check\",\n" +
//                 "            \"type\": [\"string\",\"null\"]\n" +
//                 "        }\n" +
//                 "    ]\n" +
//                 "}\n");
//         ConsumerController example = new ConsumerController("", zooKeeper, groupId, topic, json);
//         example.run(threads);
// 
      //   example.shutdown();
//     }
}
