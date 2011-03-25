package voldemort.logger.pub;

import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public interface VoldemortLogEvent {

    public ByteArray getKey();

    public Version getVersion();

    public Versioned<byte[]> getValue();

    public VoldemortEventType getType();
}
