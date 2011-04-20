package voldemort.logger.impl;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    private final Set<VectorClock> _conflictVersionSet;
    private VectorClock _maxStableVersion;
    private VectorClock _maxTentativeVersion;
    private VectorClock _mergedConcurrentVersion;

    public StabilizerUnit() {
        _maxStableVersion = new VectorClock();
        _maxTentativeVersion = new VectorClock();
        _stableEventQueue = new ArrayList<VoldemortLogEvent>();
        _tentativeEventQueue = new ArrayDeque<VoldemortLogEvent>();
        _conflictVersionSet = new HashSet<VectorClock>();
        _mergedConcurrentVersion = null;
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

        VectorClock incomingVersion = (VectorClock) event.getVersion();
        Occured occured = getOccuredPosition(incomingVersion);

        if(Occured.AFTER == occured) {
            // happy path where no conflict found with existing writes
            _maxTentativeVersion = incomingVersion.clone();

            // put this event in tentative queue
            _tentativeEventQueue.offer(event);
        } else if(Occured.CONCURRENTLY == occured) {
            // when a concurrent write arrives, itself will be discarded along
            // with any previously received ones that are concurrent with it. To
            // be able to detect any future conflicts, we keep track of all
            // conflicting versions until we see a write that supersedes them
            // all
            if(_conflictVersionSet.isEmpty()) {
                // add maxTentativeVersion in addition to the incoming version
                // if the Set is empty. Empty implies that we were previously
                // NOT in conflicting state.
                _conflictVersionSet.add(_maxTentativeVersion.clone());
                _mergedConcurrentVersion = _maxTentativeVersion.clone();
            }
            // TODO: need to optimize it by updating the BEFORE versions in
            // the conflict set, if any, instead of blindly adding the new one.
            _conflictVersionSet.add(incomingVersion.clone());
            _mergedConcurrentVersion = _mergedConcurrentVersion.merge(incomingVersion);

            // purge any concurrent writes in tentative queue
            purgeConcurrentWrites(incomingVersion);
        } else {
            // TODO: Note that it is possible for two independent puts (either
            // read-repairs or regular puts)
            // to have the same vector clock. Voldemort doesn't have a way to
            // distinguish them.
            // Need further investigation to understand how it works and a
            // solution for this case.
            logger.warn("Older or same-versioned event received with version=" + incomingVersion
                        + " current _maxTentativeVersion=" + _maxTentativeVersion
                        + " current _mergedConcurrentVersion=" + _mergedConcurrentVersion);
            logger.warn(dumpConcurrentVersions());
            throw new ObsoleteVersionException(event.toString());
        }
    }

    private String dumpConcurrentVersions() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        if(!_conflictVersionSet.isEmpty()) {
            for(VectorClock clock: _conflictVersionSet) {
                sb.append(clock.toString());
                sb.append(",");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private Occured getOccuredPosition(Version incomingVersion) {
        Occured occured = null;

        if(_conflictVersionSet.isEmpty()) {
            // we are not in conflicting state, use maxTentativeVersion for
            // comparison
            occured = incomingVersion.compare(_maxTentativeVersion);
        } else {
            // we've seen prior conflicts, compare with all of them -
            // i.e. maxTentativeVersion shall NOT be trusted in this case

            // first, assuming the incoming version is BEFORE all existing
            // concurrent write, which should have been a unusual case.
            occured = Occured.BEFORE;

            // initialize a counter for AFTER occurrences
            int numOfAfter = 0;

            // if the incoming version is CONCURRENT to any of the existing
            // concurrent versions, we return CONCURRENT
            for(VectorClock concurrentEntry: _conflictVersionSet) {
                Occured localOccured = incomingVersion.compare(concurrentEntry);
                if(Occured.CONCURRENTLY == localOccured) {
                    occured = localOccured;
                    break;
                } else if(Occured.AFTER == localOccured) {
                    numOfAfter++;
                }
            }

            if(numOfAfter == _conflictVersionSet.size()) {
                // incoming is AFTER only if it's AFTER all concurrent writes
                occured = Occured.AFTER;
            }
        }

        return occured;
    }

    private void purgeConcurrentWrites(Version incomingVersion) {
        boolean finished = false;
        Version version = _tentativeEventQueue.peekLast().getVersion();
        while(null != version && !finished) {
            // starting from the latest event, remove any event that is
            // concurrent with the incoming one since we expect to receive a
            // write repair event that supersedes those concurrent ones.
            if(Occured.CONCURRENTLY == version.compare(incomingVersion)) {
                _tentativeEventQueue.pollLast();
                version = _tentativeEventQueue.peekLast().getVersion();
            } else {
                // this event and all events before this one will be BEFORE
                // the incoming event, stop here!!!
                finished = true;
            }
        }
    }
}
