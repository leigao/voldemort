package voldemort.logger.impl;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.logger.pub.VoldemortLogEvent;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;

public class StabilizerUnit {

    private final Logger logger = Logger.getLogger(getClass());
    private final Deque<VoldemortLogEvent> _tentativeEventQueue;
    private final List<VoldemortLogEvent> _stableEventQueue;
    private VectorClock _maxStableVersion;
    private VectorClock _maxTentativeVersion;

    public StabilizerUnit() {
        _maxStableVersion = new VectorClock();
        _maxTentativeVersion = new VectorClock();
        _stableEventQueue = new ArrayList<VoldemortLogEvent>();
        _tentativeEventQueue = new ArrayDeque<VoldemortLogEvent>();
    }

    public void appendUnstableEvent(VoldemortLogEvent event) throws ObsoleteVersionException {
        // append the new event
        appendTentative(event);

        // after new event is appended, we might be able to generate more
        // stable events since we know more about what each replicas has
        // TODO: make the following process asynchronous to improve throughput
        // if needed
        processStable();
    }

    public List<VoldemortLogEvent> getStableLog() {
        return _stableEventQueue;
    }

    private void processStable() {
        boolean finished = false;
        _maxStableVersion = _maxStableVersion.merge(_maxTentativeVersion.decrementedAll());
        Version version = _tentativeEventQueue.peek().getVersion();
        while(null != version && !finished) {
            if(Occured.AFTER == _maxStableVersion.compare(version)
               || _maxStableVersion.equals(version)) {
                // move event from tentative queue to stable queue when the max
                // stable version is greater than or equal to the current events
                _stableEventQueue.add(_tentativeEventQueue.poll());
                version = _tentativeEventQueue.peek().getVersion();
            } else {
                // events after this will all have larger version, we stop here.
                finished = true;
            }
        }
    }

    private void appendTentative(VoldemortLogEvent event) throws ObsoleteVersionException {

        Version incomingVersion = event.getVersion();
        Occured occured = incomingVersion.compare(_maxTentativeVersion);

        if(Occured.AFTER == occured) {
            // happy path where no conflict found with existing writes
            _maxTentativeVersion = ((VectorClock) incomingVersion).clone();
            _tentativeEventQueue.offer(event);
        } else if(Occured.CONCURRENTLY == occured) {
            boolean finished = false;
            _maxTentativeVersion = _maxTentativeVersion.merge((VectorClock) incomingVersion);
            Version version = _tentativeEventQueue.peekLast().getVersion();
            while(null != version && !finished) {
                // starting from the latest event, remove any event that is
                // concurrent with the incoming one since we expect to receive a
                // write repair event that supersedes those concurrent ones.
                if(Occured.CONCURRENTLY == version.compare(incomingVersion)) {
                    _tentativeEventQueue.pollLast();
                    version = _tentativeEventQueue.peekLast().getVersion();
                } else {
                    // this event and all events before this one will be before
                    // the incoming event, stop here and those events will be
                    // moved to stable log
                    finished = true;
                }
            }
        } else {
            logger.warn("Older event received with version=" + incomingVersion
                        + " current _maxTentativeVersion=" + _maxTentativeVersion);
            throw new ObsoleteVersionException(event.toString());
        }
    }
}
