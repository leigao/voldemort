package voldemort.store.bdb;

import java.util.ArrayList;
import java.util.List;

import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.javaapi.producer.SyncProducer;
import kafka.message.Message;
import kafka.producer.SyncProducerConfig;

import org.apache.log4j.Logger;

import voldemort.logger.impl.AbstractLogEvent;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class StreamingBdbStorageEngine extends BdbStorageEngine {

    private static final Logger logger = Logger.getLogger(StreamingBdbStorageEngine.class.getName());

    private SyncProducer _streamer = null;
    private String _localTopic = null;

    public StreamingBdbStorageEngine(String name,
                                     Environment environment,
                                     Database database,
                                     LockMode readLockMode,
                                     boolean cursorPreload,
                                     SyncProducerConfig syncProducerConfig,
                                     String topic,
                                     boolean isLogStream) {
        super(name, environment, database, readLockMode, cursorPreload);
        if(isLogStream) {
            _streamer = new SyncProducer(syncProducerConfig);
            _localTopic = topic;
        }

        logger.info("StreamingBdbStorageEngine: " + " streamer=" + _streamer + " localTopic="
                    + _localTopic);
    }

    // TODO: make the implementation recoverable, i.e. implement write-ahead-log
    // logic.
    @Override
    protected OperationStatus putInternal(Cursor cursor,
                                          DatabaseEntry keyEntry,
                                          Versioned<byte[]> value) {

        if(null != _streamer) {
            publishEvent(keyEntry, value);
        }
        // put into bdb storage
        return super.putInternal(cursor, keyEntry, value);
    }

    private void publishEvent(DatabaseEntry keyEntry, Versioned<byte[]> value) {
        // create kafka message
        Message message = new Message((new AbstractLogEvent(new ByteArray(keyEntry.getData()),
                                                            (VectorClock) value.getVersion(),
                                                            value)).toBytes());
        List<Message> messageList = new ArrayList<Message>();
        messageList.add(message);
        ByteBufferMessageSet set = new ByteBufferMessageSet(messageList);

        // publish kafka message
        _streamer.send(_localTopic, set);
    }
}
