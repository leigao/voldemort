package voldemort.logger.impl;

import java.util.List;

import voldemort.logger.pub.PublishStateCheckpoint;
import voldemort.logger.pub.PublishStateCheckpointProvider;
import voldemort.logger.pub.VoldemortLogEvent;
import voldemort.logger.pub.VoldemortLogEventPublisher;

public class KafkaEventPublisher implements VoldemortLogEventPublisher,
        PublishStateCheckpointProvider {

    public KafkaEventPublisher() {

    }

    public void publish(List<VoldemortLogEvent> stablePrefix) {}

    public PublishStateCheckpoint readCheckpoint() {
        return null;
    }

    public void writeCheckpoint(PublishStateCheckpoint checkpoint) {}

}
