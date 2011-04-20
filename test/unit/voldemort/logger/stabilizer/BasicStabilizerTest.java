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
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class BasicStabilizerTest extends TestCase {

    private static final Logger _logger = Logger.getLogger(BasicStabilizerTest.class);
    private AbstractLogStabilizer _stabilizer;
    private List<VoldemortLogEvent> _publishLog;
    private List<VoldemortLogEvent> _trackLog;
    private DummyLogPublisher _dummyPublisher;

    @Override
    @Before
    public void setUp() throws Exception {
        _publishLog = new ArrayList<VoldemortLogEvent>();
        _trackLog = new ArrayList<VoldemortLogEvent>();
        _stabilizer = new AbstractLogStabilizer();
        _dummyPublisher = new DummyLogPublisher(_publishLog);
        _stabilizer.registerStableEventsPublisher(_dummyPublisher);
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
        assertTrue("Event published while not stable:\n" + printEventLogs(),
                   compareEventLogs(_publishLog, _trackLog));

        event = new AbstractLogEvent(key1, createVersion(2, 2, 1), value1);
        _stabilizer.appendUnstableEvent(event);

        event = new AbstractLogEvent(key1, createVersion(2, 2, 2), value1);
        _stabilizer.appendUnstableEvent(event);

        // no stable log shall be published at this time
        assertTrue("Event published while not stable after 3 events:\n" + printEventLogs(),
                   compareEventLogs(_publishLog, _trackLog));

        event = new AbstractLogEvent(key1, createVersion(3, 2, 2), value1);
        _stabilizer.appendUnstableEvent(event);

        // we shall have the first event <2, 1, 1> published
        _trackLog.add(new AbstractLogEvent(key1, createVersion(2, 1, 1), value1));
        assertTrue("One stable event not published:\n" + printEventLogs(),
                   compareEventLogs(_publishLog, _trackLog));

        event = new AbstractLogEvent(key1, createVersion(3, 3, 2), value1);
        _stabilizer.appendUnstableEvent(event);
        event = new AbstractLogEvent(key1, createVersion(3, 4, 2), value1);
        _stabilizer.appendUnstableEvent(event);
        event = new AbstractLogEvent(key1, createVersion(4, 4, 2), value1);
        _stabilizer.appendUnstableEvent(event);

        // we shall have the first event <2, 2, 1> published
        _trackLog.add(new AbstractLogEvent(key1, createVersion(2, 2, 1), value1));
        assertTrue("Two stable events not published:\n" + printEventLogs(),
                   compareEventLogs(_publishLog, _trackLog));

        event = new AbstractLogEvent(key1, createVersion(4, 4, 3), value1);
        _stabilizer.appendUnstableEvent(event);
        event = new AbstractLogEvent(key1, createVersion(4, 4, 4), value1);
        _stabilizer.appendUnstableEvent(event);

        // multible stable events are published.
        _trackLog.add(new AbstractLogEvent(key1, createVersion(2, 2, 2), value1));
        _trackLog.add(new AbstractLogEvent(key1, createVersion(3, 2, 2), value1));
        _trackLog.add(new AbstractLogEvent(key1, createVersion(3, 3, 2), value1));
        assertTrue("Many stable events not published:\n" + printEventLogs(),
                   compareEventLogs(_publishLog, _trackLog));

    }

    public void testConcurrentWrites() {
        ByteArray key1 = new ByteArray("key1".getBytes());
        Versioned<byte[]> value1 = new Versioned<byte[]>("value1".getBytes());
        VoldemortLogEvent event = new AbstractLogEvent(key1, createVersion(2, 1, 1), value1);
        _stabilizer.appendUnstableEvent(event);
        event = new AbstractLogEvent(key1, createVersion(2, 2, 1), value1);
        _stabilizer.appendUnstableEvent(event);
        event = new AbstractLogEvent(key1, createVersion(2, 2, 2), value1);
        _stabilizer.appendUnstableEvent(event);
        // no stable log shall be published at this time
        assertTrue("step1:\n" + printEventLogs(), compareEventLogs(_publishLog, _trackLog));

        // add two concurrent events, no stable events generated as the result
        event = new AbstractLogEvent(key1, createVersion(2, 2, 3), value1);
        _stabilizer.appendUnstableEvent(event);
        event = new AbstractLogEvent(key1, createVersion(2, 3, 2), value1);
        _stabilizer.appendUnstableEvent(event);
        assertTrue("step2:\n" + printEventLogs(), compareEventLogs(_publishLog, _trackLog));

        // add older events, no stable events generated as the result
        assertException(key1, createVersion(1, 1, 1), value1, "exception1");
        assertException(key1, createVersion(2, 2, 2), value1, "exception2");
        assertTrue("step3:\n" + printEventLogs(), compareEventLogs(_publishLog, _trackLog));

        // add more concurrent events (supersedes one of the existing event, but
        // not all), no stable events generated as the result
        event = new AbstractLogEvent(key1, createVersion(2, 2, 4), value1);
        _stabilizer.appendUnstableEvent(event);
        event = new AbstractLogEvent(key1, createVersion(2, 2, 5), value1);
        _stabilizer.appendUnstableEvent(event);
        event = new AbstractLogEvent(key1, createVersion(2, 4, 2), value1);
        _stabilizer.appendUnstableEvent(event);
        assertTrue("step4:\n" + printEventLogs(), compareEventLogs(_publishLog, _trackLog));

        // merged event arrives, (supersedes some, but not all concurrent
        // events). still no stable events generated
        event = new AbstractLogEvent(key1, createVersion(2, 3, 3), value1);
        _stabilizer.appendUnstableEvent(event);
        assertTrue("step5:\n" + printEventLogs(), compareEventLogs(_publishLog, _trackLog));

        // merged event arrives, (supersedes all concurrent event).
        // still no stable events generated
        event = new AbstractLogEvent(key1, createVersion(2, 4, 5), value1);
        _stabilizer.appendUnstableEvent(event);
        assertTrue("step6:\n" + printEventLogs(), compareEventLogs(_publishLog, _trackLog));

        // a larger event from first server, supersedes two initial concurrent
        // writes (e.g. <2,2,3> and <2,3,2>), but not all. no stable events are
        // generated.
        event = new AbstractLogEvent(key1, createVersion(3, 3, 3), value1);
        _stabilizer.appendUnstableEvent(event);
        assertTrue("step7:\n" + printEventLogs(), compareEventLogs(_publishLog, _trackLog));

        // a final merge event from first server advances max. version so all
        // three initial events are published
        event = new AbstractLogEvent(key1, createVersion(4, 4, 5), value1);
        _stabilizer.appendUnstableEvent(event);
        _trackLog.add(new AbstractLogEvent(key1, createVersion(2, 1, 1), value1));
        _trackLog.add(new AbstractLogEvent(key1, createVersion(2, 2, 1), value1));
        _trackLog.add(new AbstractLogEvent(key1, createVersion(2, 2, 2), value1));
        assertTrue("step8:\n" + printEventLogs(), compareEventLogs(_publishLog, _trackLog));

        // add more events so the merged event is published while the concurrent
        // ones are not.
        event = new AbstractLogEvent(key1, createVersion(4, 4, 6), value1);
        _stabilizer.appendUnstableEvent(event);
        event = new AbstractLogEvent(key1, createVersion(4, 5, 6), value1);
        _stabilizer.appendUnstableEvent(event);
        event = new AbstractLogEvent(key1, createVersion(5, 5, 6), value1);
        _stabilizer.appendUnstableEvent(event);
        event = new AbstractLogEvent(key1, createVersion(5, 5, 7), value1);
        _stabilizer.appendUnstableEvent(event);
        _trackLog.add(new AbstractLogEvent(key1, createVersion(4, 4, 5), value1));
        _trackLog.add(new AbstractLogEvent(key1, createVersion(4, 4, 6), value1));
        assertTrue("step9:\n" + printEventLogs(), compareEventLogs(_publishLog, _trackLog));
    }

    private VectorClock createVersion(int... clockValues) {
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

    private String printEventLogs() {
        StringBuilder st = new StringBuilder();
        st.append("published events ================== \n");
        for(VoldemortLogEvent event: _publishLog) {
            st.append(new String(event.getKey().get()) + ":" + event.getVersion() + "\n");
        }
        st.append("\n\n");
        st.append("expected events =================== \n");
        for(VoldemortLogEvent event: _trackLog) {
            st.append(new String(event.getKey().get()) + ":" + event.getVersion() + "\n");
        }

        if(_dummyPublisher.hadErrors()) {
            st.append(_dummyPublisher.getErrMsg());
        }

        return st.toString();
    }

    private void assertException(ByteArray key1,
                                 VectorClock version,
                                 Versioned<byte[]> value1,
                                 String msg) {
        boolean exceptionOccured = false;
        try {
            VoldemortLogEvent event = new AbstractLogEvent(key1, version, value1);
            _stabilizer.appendUnstableEvent(event);
        } catch(ObsoleteVersionException e) {
            exceptionOccured = true;
        } finally {
            assertTrue(msg, exceptionOccured);
        }
    }
}
