package voldemort.logger.pub;

import java.util.List;

public interface StableEventPublisher {

    public void publish(List<VoldemortLogEvent> stablePrefix);
}
