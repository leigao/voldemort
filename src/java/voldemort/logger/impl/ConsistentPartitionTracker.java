package voldemort.logger.impl;

import java.util.HashMap;
import java.util.Map;

import voldemort.cluster.Cluster;
import voldemort.logger.pub.ConsistencyUnitTracker;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;

public class ConsistentPartitionTracker implements ConsistencyUnitTracker {

    private final Map<Integer, StabilizerUnit> _consistentPartitions;
    private final ConsistentRoutingStrategy _routingStrategy;

    public ConsistentPartitionTracker(Cluster cluster, StoreDefinition storeDef) {
        _consistentPartitions = new HashMap<Integer, StabilizerUnit>();
        _routingStrategy = new ConsistentRoutingStrategy(cluster.getNodes(),
                                                         storeDef.getReplicationFactor());
    }

    public void createTrackedUnit(ByteArray key, long idleTimeout) {
        Integer partitionId = _routingStrategy.getPrimaryPartiion(key.get());
        StabilizerUnit partitionUnit = new StabilizerUnit(partitionId.intValue(), idleTimeout);
        _consistentPartitions.put(partitionId, partitionUnit);
    }

    public StabilizerUnit getTrackedUnit(ByteArray key) {
        Integer partitionId = _routingStrategy.getPrimaryPartiion(key.get());
        return _consistentPartitions.get(partitionId);
    }

    public boolean isUnitTracked(ByteArray key) {
        Integer partitionId = _routingStrategy.getPrimaryPartiion(key.get());
        return _consistentPartitions.containsKey(partitionId);
    }

    public void onIdleTimerExpires(StabilizerUnit idleUnit) {}

}
