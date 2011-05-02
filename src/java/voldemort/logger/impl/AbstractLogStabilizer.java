package voldemort.logger.impl;

import java.util.List;

import voldemort.logger.pub.ConsistencyUnitTracker;
import voldemort.logger.pub.LogStabilizer;
import voldemort.logger.pub.PublisherStateTracker;
import voldemort.logger.pub.VoldemortLogEvent;
import voldemort.logger.pub.VoldemortLogEventPublisher;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;

public class AbstractLogStabilizer implements LogStabilizer {

    // 60 seconds idle timeout
    private static final long IDLE_UNIT_TIMEOUT = 60000;

    private ConsistencyUnitTracker _consistencyUnitTracker;
    private VoldemortLogEventPublisher _eventPublisher;
    private PublisherStateTracker _publisherStateTracker;

    public AbstractLogStabilizer() {
        _consistencyUnitTracker = new ConsistencyUnitTrackerFactory().create();
    }

    public void appendUnstableEvent(VoldemortLogEvent event) throws ObsoleteVersionException {
        ByteArray key = event.getKey();

        if(!_consistencyUnitTracker.isUnitTracked(key)) {
            _consistencyUnitTracker.createTrackedUnit(key, IDLE_UNIT_TIMEOUT);
        }
        StabilizerUnit trackedUnit = _consistencyUnitTracker.getTrackedUnit(key);

        trackedUnit.appendUnstableEvent(event);
        List<VoldemortLogEvent> stablePrefix = trackedUnit.getStableLog();
        _eventPublisher.publish(stablePrefix);
        stablePrefix.clear();

        _publisherStateTracker.updateState(trackedUnit);
    }

    public void registerStableEventsPublisher(VoldemortLogEventPublisher publisher) {
        _eventPublisher = publisher;
    }

}
