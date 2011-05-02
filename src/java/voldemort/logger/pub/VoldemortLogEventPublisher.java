package voldemort.logger.pub;

import java.util.List;

public interface VoldemortLogEventPublisher {

    public void publish(List<VoldemortLogEvent> stablePrefix);
}
