package voldemort.logger.pub;

import voldemort.logger.impl.StabilizerUnit;

public interface PublisherStateTracker {

    public void updateState(StabilizerUnit stabilizerUnit);

    public void publishCheckpoint(PublishStateCheckpointProvider provider);

    public void reinit(PublishStateCheckpointProvider provider);
}
