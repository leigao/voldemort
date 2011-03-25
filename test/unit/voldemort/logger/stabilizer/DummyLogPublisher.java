package voldemort.logger.stabilizer;

import java.util.List;

import voldemort.logger.pub.StableEventPublisher;
import voldemort.logger.pub.VoldemortLogEvent;

public class DummyLogPublisher implements StableEventPublisher {

    private final List<VoldemortLogEvent> _eventLog;

    public DummyLogPublisher(List<VoldemortLogEvent> eventLog) {
        _eventLog = eventLog;
    }

    public void publish(List<VoldemortLogEvent> stablePrefix) {
        _eventLog.addAll(stablePrefix);
    }

}
