package voldemort.store.bdb;

import java.util.Properties;

import kafka.producer.SyncProducerConfig;
import voldemort.server.VoldemortConfig;

import com.sleepycat.je.Database;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;

public class StreamingBdbStorageConfiguration extends BdbStorageConfiguration {

    public static final String TYPE_NAME = "bdb-log";
    private SyncProducerConfig _streamerConfig;
    private boolean _isLogStream;

    public StreamingBdbStorageConfiguration(VoldemortConfig config) {
        super(config);
        Properties props = new Properties();
        props.put("host", config.getStreamerServerURL());
        props.put("port", String.valueOf(config.getStreamerServerPort()));
        props.put("buffer.size", String.valueOf(config.getStreamerBufferSize()));
        props.put("connect.timeout.ms", String.valueOf(config.getStreamerConnectionTimeout()));
        props.put("reconnect.interval", String.valueOf(config.getStreamerReconnectInterval()));
        _streamerConfig = new SyncProducerConfig(props);
        _isLogStream = true;
    }

    @Override
    protected BdbStorageEngine createEngine(String storeName,
                                            Environment environment,
                                            Database db,
                                            LockMode readLockMode,
                                            boolean bdbCursorPreload) {

        return new StreamingBdbStorageEngine(storeName,
                                             environment,
                                             db,
                                             readLockMode,
                                             bdbCursorPreload,
                                             _streamerConfig,
                                             /*
                                              * TODO: pass in the right partiton
                                              * name
                                              */"P1",
                                             _isLogStream);
    }

    @Override
    public String getType() {
        return TYPE_NAME;
    }

    @Override
    public void close() {
        super.close();
        // TODO: close any kafka environment related stuff.
    }

    public void setLogStream(boolean isLogStream) {
        _isLogStream = isLogStream;
    }

}
