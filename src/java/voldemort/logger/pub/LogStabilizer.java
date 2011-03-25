package voldemort.logger.pub;


public interface LogStabilizer {

    public void appendUnstableEvent(VoldemortLogEvent event);

}
