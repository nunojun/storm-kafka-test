package bumjoon;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.kafka.FailedFetchException;
import storm.kafka.KafkaUtils;
import storm.kafka.Partition;
import storm.kafka.trident.GlobalPartitionInformation;
import storm.kafka.trident.IBrokerReader;
import storm.kafka.trident.TridentKafkaConfig;
import storm.kafka.trident.TridentKafkaEmitter;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;

public class SimpleKafkaSpout implements IOpaquePartitionedTridentSpout<GlobalPartitionInformation, Partition, Map>{

	private static final Logger LOG = Logger.getLogger(SimpleScheme.class);
	
    private TridentKafkaConfig config;
    private String topologyInstanceId;

    public SimpleKafkaSpout(TridentKafkaConfig config) {
    	this.config = config;
    	this.topologyInstanceId = UUID.randomUUID().toString();
    }
    
	@Override
	public storm.trident.spout.IOpaquePartitionedTridentSpout.Emitter<GlobalPartitionInformation, Partition, Map> getEmitter(
			Map conf, TopologyContext context) {
		return new Emitter(conf, context, this.config, this.topologyInstanceId);
	}

	@Override
	public storm.trident.spout.IOpaquePartitionedTridentSpout.Coordinator getCoordinator(Map conf,
			TopologyContext context) {
		return new Coordinator(conf, this.config);
	}

	@Override
	public Map getComponentConfiguration() {
		return null;
	}

	@Override
	public Fields getOutputFields() {
        return this.config.scheme.getOutputFields();
	}
	
	class Emitter implements IOpaquePartitionedTridentSpout.Emitter<GlobalPartitionInformation, Partition, Map> {

		private TridentKafkaEmitter tridentKafkaEmitter;
		private IOpaquePartitionedTridentSpout.Emitter<GlobalPartitionInformation, Partition, Map> delegateEmitter;
		private Method method;
		
		public Emitter(Map conf, TopologyContext context, TridentKafkaConfig config, String topologyInstanceId) {
			this.tridentKafkaEmitter = new TridentKafkaEmitter(conf, context, config, topologyInstanceId);
			this.delegateEmitter = this.tridentKafkaEmitter.asOpaqueEmitter();
			this.initMethod();
		}
		
		private void initMethod() {
			try {
				this.method = this.tridentKafkaEmitter.getClass().getDeclaredMethod(
						"failFastEmitNewPartitionBatch",
						Class.forName("storm.trident.topology.TransactionAttempt"),
						Class.forName("storm.trident.operation.TridentCollector"),
						Class.forName("storm.kafka.Partition"),
						Class.forName("java.util.Map"));
			} catch (NoSuchMethodException | SecurityException | ClassNotFoundException e) {
				LOG.error(e.getLocalizedMessage(), e);
				this.method = null;
				throw new RuntimeException(e);
			}
			this.method.setAccessible(true);
		}
		
		@Override
		public List<Partition> getOrderedPartitions(GlobalPartitionInformation allPartitionInfo) {
			return this.delegateEmitter.getOrderedPartitions(allPartitionInfo);
		}

		@Override
		public void refreshPartitions(List<Partition> partitionResponsibilities) {
			this.delegateEmitter.refreshPartitions(partitionResponsibilities);			
		}

		@Override
		public Map emitPartitionBatch(TransactionAttempt attempt, TridentCollector collector, Partition partition,
				Map lastMeta) {
			if (this.method == null) {
				LOG.error("bkimtest : method is null!!");
				return null;
			}
			
			Object result = null;
			
			try {
				result = this.method.invoke(this.tridentKafkaEmitter,
						attempt, collector, partition, lastMeta);
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				if (e instanceof InvocationTargetException &&
						e.getCause() instanceof FailedFetchException) {
					LOG.error("bkimtest : this is what I expected!!");
					// TODO
					// System.exit(0);
				}
				LOG.error(e.getLocalizedMessage(), e);
				return null;
			}
			
			if (result != null && result instanceof Map) {
				return (Map) result;
			}
			return null;
		}

		@Override
		public void close() {
			this.delegateEmitter.close();
		}
	}

	class Coordinator implements IOpaquePartitionedTridentSpout.Coordinator<GlobalPartitionInformation> {

	    private IBrokerReader reader;
	    private TridentKafkaConfig config;

	    public Coordinator(Map conf, TridentKafkaConfig tridentKafkaConfig) {
	        this.config = tridentKafkaConfig;
	        this.reader = KafkaUtils.makeBrokerReader(conf, config);
	    }
	    
	    @Override
	    public void close() {
	        this.config.coordinator.close();
	    }

	    @Override
	    public boolean isReady(long txid) {
	        return this.config.coordinator.isReady(txid);
	    }

	    @Override
	    public GlobalPartitionInformation getPartitionsForBatch() {
	        return this.reader.getCurrentBrokers();
	    }
	}
}
