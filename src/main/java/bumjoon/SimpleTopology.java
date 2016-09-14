package bumjoon;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import storm.trident.testing.CountAsAggregator;

public class SimpleTopology {
	
	private static final Logger LOG = Logger.getLogger(SimpleScheme.class);

	private static final String KAFKA_ZOOKEEPER_HOST = "127.0.0.1:2181";
	private static final String KAFKA_INPUT_TOPIC = "test_input_001";
	private static final String KAFKA_CONSUMER_GROUP_ID = "001";
	private static final int KAFKA_FETCH_SIZE_BYTES = 3000000;
	private static final int KAFKA_BUFFER_SIZE_BYTES = 3000000;
	private static final int STORM_NUM_WORKERS = 1;
	private static final int STORM_NUM_ACKERS = 1;
	private static final int STORM_MAX_SPOUT_PENDING = 1;

	private TridentKafkaConfig tridentKafkaConfig;
	private OpaqueTridentKafkaSpout opaqueTridentKafkaSpout;
	private Config config;
	
	public SimpleTopology() {
		this.init();
	}
	
	private void init() {
		LOG.error("bkimtest : init()");
		this.tridentKafkaConfig = new TridentKafkaConfig(
				new ZkHosts(KAFKA_ZOOKEEPER_HOST),
				KAFKA_INPUT_TOPIC,
				KAFKA_CONSUMER_GROUP_ID);
		this.tridentKafkaConfig.scheme = new SchemeAsMultiScheme(new SimpleScheme());
		this.tridentKafkaConfig.fetchSizeBytes = KAFKA_FETCH_SIZE_BYTES;
		this.tridentKafkaConfig.bufferSizeBytes = KAFKA_BUFFER_SIZE_BYTES;
		this.tridentKafkaConfig.forceFromStart = false;
		
		this.opaqueTridentKafkaSpout = new OpaqueTridentKafkaSpout(this.tridentKafkaConfig);
		
		this.config = new Config();
		this.config.setNumWorkers(STORM_NUM_WORKERS);
		this.config.setNumAckers(STORM_NUM_ACKERS);
		this.config.setMaxSpoutPending(STORM_MAX_SPOUT_PENDING);
		
	}
	
	public void execute() {
		LOG.error("bkimtest : execute()");
		TridentTopology topology = new TridentTopology();

		topology.newStream(KAFKA_INPUT_TOPIC, this.opaqueTridentKafkaSpout).name("kafkaSpout")
		.aggregate(new Fields("A", "B", "C", "D"), new CountAsAggregator(), new Fields("A", "B", "C", "D"));
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("SimpleTopology", this.config, topology.build());
		/*
		try {
			StormSubmitter.submitTopology("SimpleTopology", this.config, topology.build());
		} catch (AlreadyAliveException | InvalidTopologyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
	}
	
	public static void main(String[] args) {
		SimpleTopology topology = new SimpleTopology();
		topology.execute();
	}
}
