package zork.agent.module.consumercore.baseconsumers.baselowlevelavroconsumer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import zork.agent.module.consumercore.interfaces.AvroDeserializer;
import zork.agent.module.consumercore.utils.zkutil.CuratorFrameworkZkUtils;
import zork.agent.module.consumercore.baseanalytical.BaseAvroAnalytical;
import zork.agent.utility.LogHelper;

/**
 * 
 * @author Sin.Z
 *
 */
public class BaseAvroFetchController extends BaseFetchController implements AvroDeserializer {
	public JSONObject jsonObject;
	public JSONArray jsonArray;
	public Schema schema;
	public String[] keys;
	public StringBuilder stringBuilder;

    public BaseAvroFetchController(int partitionNum, String scheama, CuratorFrameworkZkUtils zkUtils) {
        super(partitionNum, zkUtils);
		this.scheduExec = Executors.newScheduledThreadPool(partitionNum);
		getKeysfromjson(scheama);
	}

	@Override
	public void getKeysfromjson(String scheama) {
		this.jsonObject = JSONObject.parseObject(scheama);
		this.schema = new Schema.Parser().parse(scheama);
		this.jsonArray = this.jsonObject.getJSONArray("fields");
		this.keys = new String[this.jsonArray.size()];
		for (int i = 0; i < this.jsonArray.size(); i++) {
			this.keys[i] = this.jsonArray.getJSONObject(i).get("name").toString();
		}
	}

	@Override
	public GenericRecord deserializing(byte[] body) {
		this.stringBuilder = new StringBuilder();
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(this.schema);
		Decoder decoder = DecoderFactory.get().binaryDecoder(body, null);
		GenericRecord result = null;
		try {
			result = datumReader.read(null, decoder);
		} catch (IOException e) {
			LogHelper.info(String.format("error Avro范序列化 �?%s", e), true);
		}
		return result;
	}
}
