package voldemort.logger.pub;

import voldemort.logger.impl.StabilizerUnit;
import voldemort.utils.ByteArray;

public interface ConsistencyUnitTracker {

    void createTrackedUnit(ByteArray key, long idleTimer);

    StabilizerUnit getTrackedUnit(ByteArray key);

    boolean isUnitTracked(ByteArray key);

    void onIdleTimerExpires(StabilizerUnit idleUnit);
}
