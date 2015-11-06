package zork.agent.module.consumercore.baseconsumers.basethreadconsumer;


import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
/**
* @ClassName  : BaseSubConsumer 
* @Description  : TODO KafkaConsumer的基类，开启一个线程消费一个指定的topic，用来消费kafka消息。
* @author  :suyan
* @email  :suyan@zork.com.cn
*/
@SuppressWarnings(value = {"unused","unchecked", "rawtypes", })
public class BaseSubConsumer  implements Runnable{
	
	/** 
	* @Fields m_stream : TODO  kafka consumer 流
	*/ 
	private KafkaStream<byte[], byte[]> m_stream;
	/** 
	* @Fields m_threadNumber : TODO  线程数和partition 数量一致
	*/ 
	private int m_threadNumber;
	/** 
	* @Fields connector : TODO  kafka consumer 链接
	*/ 
	private  ConsumerConnector connector;
	/** 
	* @Fields jsonObject : TODO  json 对象，用于转换
	*/ 
	private JSONObject jsonObject; 
    /** 
    * @Fields jsonArray : TODO  json 数组，用于转换
    */ 
    private JSONArray jsonArray;
   
   
    /** 
    * <p>Title: </p> 构造函数
    * <p>Description: </p> 构造一个BaseSubConsumer 方法
    * @param a_stream kafka consumer流
    * @param a_threadNumber 线程数
    * @param connector  kafka consumer 链接
    */

	public BaseSubConsumer( KafkaStream a_stream,int a_threadNumber,ConsumerConnector connector) {
	    this.m_stream = a_stream;
	    this.m_threadNumber = a_threadNumber;
	    this.connector=connector;
	}
	
	/* (non Javadoc)
	* Title: run
	* Description: 线程运行入口
	* @see java.lang.Runnable#run()
	*/
	@Override
	public void run() {
	   ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
	   while(it.hasNext()){
		   //System.out.println("Thread "+ m_threadNumber + " :"+new String(it.next().message())+"-id:"+Thread.currentThread().getId());
		   processData(new String((it.next().message())));
	   }
	   System.out.println("Shutting down Thread:"+m_threadNumber);
	}
	
	/** 
	* @Title: processData 
	* @Description: TODO用户kafkaconusmer 接收数据后的处理方法
	* @param message    设定文件 
	* void    返回类型 
	* @throws 
	*/
	public void processData(String message) {
		System.out.println(message);
	}
}
