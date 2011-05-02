package voldemort.logger.pub;

public interface PublishStateCheckpointProvider {

    public void writeCheckpoint(PublishStateCheckpoint checkpoint);

    public PublishStateCheckpoint readCheckpoint();
}
