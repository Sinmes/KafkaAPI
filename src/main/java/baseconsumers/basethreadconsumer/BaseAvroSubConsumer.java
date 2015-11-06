package zork.agent.module.consumercore.baseconsumers.basethreadconsumer;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import zork.agent.module.consumercore.interfaces.AvroDeserializer;

/**
* @ClassName  : BaseAvroSubConsumer
* @Description  : TODO  kafka consumer 子线程
* @author  :suyan
* @email  :suyan@zork.com.cn
*/
@SuppressWarnings(value={"unused","rawtypes", "unchecked"})
public class BaseAvroSubConsumer implements Runnable, AvroDeserializer{
	/** 
	* @Fields m_stream : TODO kafka 数据流
	*/ 
	private KafkaStream<byte[], byte[]> m_stream;
	/** 
	* @Fields m_threadNumber : TODO  kafka处理线程数
	*/ 
	private int m_threadNumber;
	/** 
	* @Fields connector : TODO  kafka consumer 链接
	*/ 
	private  ConsumerConnector connector;
	/** 
	* @Fields jsonObject : TODO  json 对象
	*/ 
	private JSONObject jsonObject;
    /** 
    * @Fields jsonArray : TODO  json 数组
    */ 
    private JSONArray jsonArray;
    /** 
    * @Fields schema : TODO 用户avro序列化和反序列化的schema 
    */ 
    private Schema schema; 
    /** 
    * @Fields keys : TODO 通过解析得到的keys
    */ 
    private String[] keys;
    /** 
    * @Fields stringBuilder : TODO 
    */ 
    private StringBuilder  stringBuilder = new StringBuilder();
   
	public BaseAvroSubConsumer(KafkaStream a_stream,int a_threadNumber,ConsumerConnector connector, String scheam) {
	    this.m_stream = a_stream;
	    this.m_threadNumber = a_threadNumber;
	    this.connector=connector;
	    getKeysfromjson(scheam);
	}
	
	/* (non Javadoc)
	* Title: run
	* Description: 线程运行方法
	* @see java.lang.Runnable#run()
	*/
	@Override
	public void run() {
	   ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
	   while(it.hasNext()){
		   //connector.commitOffsets(true);
		   System.out.println("Thread "+ m_threadNumber + " :"+new String(it.next().message())+"-id:"+Thread.currentThread().getId());
		   processData(deserializing(it.next().message()));
	   }
	   System.out.println("Shutting down Thread:"+m_threadNumber);
	}
	
	/** 
	* @Title: processData 
	* @Description: TODO consumer 数据处理方法
	* @param genericRecord    设定文件 
	* void    返回类型 
	* @throws 
	*/
	public void processData(GenericRecord  genericRecord) {
		System.out.println(genericRecord);
	}

	/* (non Javadoc)
	* Title: getKeysfromjson 
	* Description:通过avro 解析得到key
	* @param scheama 用于avro解析的scheama
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
	

    /** 
    * @Title: getKeys 
    * @Description: TODO可用此方法获得消息各字段key。
    * @return    设定文件 
    * String[]    消息的keys。
    * @throws 
    */
    public String[] getKeys(){
        return this.keys;
    }

	/* (non Javadoc)
	* Title: deserializing
	* Description: avro 反序列化
	* @param body 消息体
	* @return
	* @see zork.agent.module.consumers.baseConsumer.deserializer.AvroDeserializer#deserializing(byte[])
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
}
