package voldemort.logger.impl;

import voldemort.logger.pub.VoldemortEventType;
import voldemort.logger.pub.VoldemortLogEvent;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class AbstractLogEvent implements VoldemortLogEvent {

    private final Version _version;
    private final ByteArray _key;
    private final Versioned<byte[]> _value;
    private VoldemortEventType _type;

    public AbstractLogEvent(ByteArray key, Version version, Versioned<byte[]> value) {
        _key = key;
        _version = version;
        _value = value;
        _type = VoldemortEventType.tentative;
    }

    public ByteArray getKey() {
        return _key;
    }

    public VoldemortEventType getType() {
        return _type;
    }

    public Versioned<byte[]> getValue() {
        return _value;
    }

    public Version getVersion() {
        return _version;
    }

    @Override
    public boolean equals(Object otherEvent) {
        if(this == otherEvent) {
            return true;
        } else if(null == otherEvent || !(otherEvent instanceof AbstractLogEvent)) {
            return false;
        } else {
            AbstractLogEvent event = (AbstractLogEvent) otherEvent;
            return (_key.equals(event.getKey()) && _value.equals(event.getValue())
                    && _type.equals(event.getType()) && ((VectorClock) _version).equals(event.getVersion()));
        }
    }
}
