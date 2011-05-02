package voldemort.logger.stabilizer;

import java.util.List;

import voldemort.logger.pub.VoldemortLogEvent;
import voldemort.logger.pub.VoldemortLogEventPublisher;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;

public class DummyLogPublisher implements VoldemortLogEventPublisher {

    private final List<VoldemortLogEvent> _eventLog;
    private StringBuilder _errMsg;
    private boolean _hadErrors;

    public DummyLogPublisher(List<VoldemortLogEvent> eventLog) {
        _eventLog = eventLog;
        _errMsg = new StringBuilder();
        _hadErrors = false;
    }

    public void publish(List<VoldemortLogEvent> stablePrefix) {
        VectorClock lastVersion = new VectorClock();
        for(VoldemortLogEvent event: stablePrefix) {
            _eventLog.add(event);
            if(Occured.AFTER != event.getVersion().compare(lastVersion)) {
                _hadErrors = true;
                _errMsg.append("Out of order event seen while publishing stable events:\n");
                _errMsg.append("lastVersion=" + lastVersion + "\n");
                _errMsg.append("incoming version =" + event.getVersion() + "\n");
            } else {
                lastVersion = ((VectorClock) event.getVersion()).clone();
            }
        }
    }

    public boolean hadErrors() {
        return _hadErrors;
    }

    public String getErrMsg() {
        return _errMsg.toString();
    }
}
