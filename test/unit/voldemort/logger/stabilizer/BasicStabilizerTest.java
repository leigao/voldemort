package voldemort.logger.stabilizer;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.logger.impl.AbstractLogEvent;
import voldemort.logger.impl.AbstractLogStabilizer;
import voldemort.logger.pub.VoldemortLogEvent;
import voldemort.utils.ByteArray;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class BasicStabilizerTest extends TestCase {

    private static final Logger _logger = Logger.getLogger(BasicStabilizerTest.class);
    private AbstractLogStabilizer _stabilizer;
    private List<VoldemortLogEvent> _publishLog;
    private List<VoldemortLogEvent> _trackLog;

    @Override
    @Before
    public void setUp() throws Exception {
        _publishLog = new ArrayList<VoldemortLogEvent>();
        _trackLog = new ArrayList<VoldemortLogEvent>();
        _stabilizer = new AbstractLogStabilizer();
        _stabilizer.registerStableEventsPublisher(new DummyLogPublisher(_publishLog));
    }

    @Override
    @After
    public void tearDown() throws Exception {}

    @Test
    public void testNoConcurrentWrites() {
        ByteArray key1 = new ByteArray("key1".getBytes());
        Versioned<byte[]> value1 = new Versioned<byte[]>("value1".getBytes());
        VoldemortLogEvent event = new AbstractLogEvent(key1, createVersion(2, 1, 1), value1);
        _stabilizer.appendUnstableEvent(event);

        // no stable log shall be published at this time
        assertTrue("Event published while not stable.", compareEventLogs(_publishLog, _trackLog));

        event = new AbstractLogEvent(key1, createVersion(2, 2, 1), value1);
        _stabilizer.appendUnstableEvent(event);

        event = new AbstractLogEvent(key1, createVersion(2, 2, 2), value1);
        _stabilizer.appendUnstableEvent(event);

        // no stable log shall be published at this time
        assertTrue("Event published while not stable after 3 events.",
                   compareEventLogs(_publishLog, _trackLog));

        event = new AbstractLogEvent(key1, createVersion(3, 2, 2), value1);
        _stabilizer.appendUnstableEvent(event);

        // we shall have the first event <1, 0, 0> published
        _trackLog.add(new AbstractLogEvent(key1, createVersion(2, 1, 1), value1));
        assertTrue("One stable event not published.", compareEventLogs(_publishLog, _trackLog));
    }

    private Version createVersion(int... clockValues) {
        List<ClockEntry> clockEntries = new ArrayList<ClockEntry>(clockValues.length);

        for(short i = 0; i < clockValues.length; i++) {
            clockEntries.add(new ClockEntry(i, clockValues[i]));
        }

        return new VectorClock(clockEntries, System.currentTimeMillis());
    }

    private boolean compareEventLogs(List<VoldemortLogEvent> thisLog,
                                     List<VoldemortLogEvent> thatLog) {
        if(null == thisLog || null == thatLog) {
            return false;
        } else {
            return thisLog.equals(thatLog);
        }
    }
}
