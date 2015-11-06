package zork.agent.module.consumercore.baseconsumers.baseconsumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import zork.agent.module.consumercore.interfaces.AvroDeserializer;
import zork.agent.utility.LogHelper;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 
 * @author Sin.Z
 *
 */
public class BaseAvroConsumer implements Runnable, AvroDeserializer {
	String classnameString=String.format("[%s] ","BaseAvroConsumer"); // 格式化字符串

    /** 
    * @Fields connector : TODO consumer 链接
    */ 
    private final ConsumerConnector connector;
    /** 
    * @Fields topic : TODO  kafka topic
    */ 
    private final String topic;
    /** 
    * @Fields jsonObject : TODO  json 对象
    */ 
    private JSONObject jsonObject;
    /** 
    * @Fields jsonArray : TODO json 数组
    */ 
    private JSONArray jsonArray;
    /** 
    * @Fields schema : TODO 用于avro解析的schema
    */ 
    private Schema schema;
    /** 
    * @Fields keys : TODO avro解析key
    */ 
    private String[] keys; 
    /** 
    * @Fields stringBuilder : TODO  
    */ 
    private StringBuilder  stringBuilder = new StringBuilder();

   
    /** 
    * <p>Title: </p> 构造函数
    * <p>Description: </p> avro 序列化consumer
    * @param zk zookeeper地址
    * @param groupID Consumer所属groupID
    * @param topic 所要消费的topic
    * @param scheam avro scheam
    */
    public BaseAvroConsumer(String zk, String groupID, String topic, String scheam) {
        connector = Consumer.createJavaConsumerConnector(createConsumerConfig(zk, groupID));
        this.topic = topic;
        getKeysfromjson(scheam);
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
    private ConsumerConfig createConsumerConfig(String zk,String groupID){

        Properties props = new Properties();
        props.put("zookeeper.connect", zk);
        props.put("group.id",groupID);
        props.put("zookeeper.session.timeout.ms","4000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        return new ConsumerConfig(props);
    }
    
    /* (non Javadoc)
    * Title: getKeysfromjson 
    * Description: 实现AvroDeserializer接口的方法，用于获取Avro的keys。
    * @param scheama  Avro序列化所使用的scheam。
    * @see zork.agent.module.consumers.baseConsumer.deserializer.AvroDeserializer#getKeysfromjson(java.lang.String)
    */
    @Override
    public void getKeysfromjson(String scheama) {
        this.jsonObject = JSONObject.parseObject(scheama);
        this.schema = new Schema.Parser().parse(scheama);
        jsonArray = this.jsonObject.getJSONArray("fields");
        keys = new String[jsonArray.size()];
        for (int i = 0; i < jsonArray.size(); i++) {
            keys[i] = jsonArray.getJSONObject(i).get("name").toString();
        }
    }

   
    /* (non Javadoc)
    * Title: deserializing
    * Description: ：实现AvroDeserializer接口的方法，用于Avro的反序列化。
    * @param body kafka消息。
    * @return 经过Avro反序列化处理的body。
    * @see zork.agent.module.consumercore.baseConsumer.deserializer.AvroDeserializer#deserializing(byte[])
    */
    @Override
    public GenericRecord deserializing(byte[] body) {
        stringBuilder.setLength(0);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(
                schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(body, null);
        GenericRecord result = null;
        try {
            result = datumReader.read(null, decoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    /** 
    * @Title: processData 
    * @Description: TODO 用户继承此类后需要复写次方法，来对消息做定制的处理，如果不复写，则简单输出。
    * @param genericRecord    avro 序列化记录 
    * void    无返回类型 
    * @throws 
    */
    public void processData(GenericRecord  genericRecord) {
        System.out.println(genericRecord);
    }
  
    /** 
    * @Title: getKeys 
    * @Description: TODO 可用此方法获得消息各字段key。
    * @return    设定文件 
    * String[]    返回类型 
    * @throws 
    */
    public String[] getKeys(){
        return this.keys;
    }

  
	/* (non Javadoc)
	* Title: run
	* Description: 线程的主方法，用于从kafka获取消息，并做反序列化处理，再调用processData()方法做处理，
	* @see java.lang.Runnable#run()
	*/
	@Override
	public void run() {
		// TODO Auto-generated method stub
			LogHelper.info(classnameString + Thread.currentThread().getName() + " 初始化!", true);
	        Map<String,Integer> maps = new HashMap<String, Integer>();
	        maps.put(topic,new Integer(1));
	        Map<String,List<KafkaStream<byte[], byte[]>>> consumerMap = connector.createMessageStreams(maps);
	        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
	        ConsumerIterator<byte[], byte[]> it= stream.iterator();
	        while(it.hasNext()){
	        	processData(deserializing(it.next().message()));
	        }
	}
}
