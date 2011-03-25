package voldemort.logger.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.logger.pub.LogStabilizer;
import voldemort.logger.pub.StableEventPublisher;
import voldemort.logger.pub.VoldemortLogEvent;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;

public class AbstractLogStabilizer implements LogStabilizer {

    private/* ConsistencyUnitsTracker */Map<ByteArray, StabilizerUnit> _consistencyUnitsTracker;
    private StableEventPublisher _stableEventPublisher;

    public AbstractLogStabilizer() {
        _consistencyUnitsTracker = new HashMap<ByteArray, StabilizerUnit>();
    }

    public void appendUnstableEvent(VoldemortLogEvent event) throws ObsoleteVersionException {
        StabilizerUnit trackedUnit = _consistencyUnitsTracker.get(event.getKey());
        if(null == trackedUnit) {
            trackedUnit = new StabilizerUnit();
            _consistencyUnitsTracker.put(event.getKey(), trackedUnit);
        }
        trackedUnit.appendUnstableEvent(event);
        List<VoldemortLogEvent> stablePrefix = trackedUnit.getStableLog();
        _stableEventPublisher.publish(stablePrefix);
    }

    public void registerStableEventsPublisher(StableEventPublisher publisher) {
        _stableEventPublisher = publisher;
    }

}
