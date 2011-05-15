package voldemort.logger.impl;

import voldemort.logger.pub.VoldemortEventType;
import voldemort.logger.pub.VoldemortLogEvent;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class AbstractLogEvent implements VoldemortLogEvent {

    private final VectorClock _version;
    private final ByteArray _key;
    private final Versioned<byte[]> _value;
    private VoldemortEventType _type;

    public AbstractLogEvent(ByteArray key, VectorClock version, Versioned<byte[]> value) {
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

    @Override
    public String toString() {
        StringBuilder st = new StringBuilder();
        st.append("key=" + new String(_key.get()) + " ");
        st.append("version=" + _version + " ");
        st.append("value=" + _value + " ");
        return st.toString();
    }

    public byte[] toBytes() {
        byte[] keyBytes = _key.get();
        byte[] versionBytes = _version.toBytes();
        byte[] valueBytes = _value.getValue();
        byte[] eventBytes = new byte[keyBytes.length + versionBytes.length + valueBytes.length];
        int eventBytesPosition = 0;
        System.arraycopy(keyBytes, 0, eventBytes, eventBytesPosition, keyBytes.length);
        eventBytesPosition += keyBytes.length;
        System.arraycopy(versionBytes, 0, eventBytes, eventBytesPosition, versionBytes.length);
        eventBytesPosition += versionBytes.length;
        System.arraycopy(valueBytes, 0, eventBytes, eventBytesPosition, valueBytes.length);
        eventBytesPosition += valueBytes.length;
        return eventBytes;
    }
}
