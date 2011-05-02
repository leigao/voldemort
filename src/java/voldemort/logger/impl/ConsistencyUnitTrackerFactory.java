package voldemort.logger.impl;

import voldemort.logger.pub.ConsistencyUnitTracker;

public class ConsistencyUnitTrackerFactory {

    // TODO: factory pattern for creating different kinds of trackers
    public ConsistencyUnitTracker create() {
        return new ConsistentPartitionTracker(null, null);
    }

}
