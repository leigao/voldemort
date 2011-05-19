package voldemort.store.versionedpartition;

import java.util.ArrayList;
import java.util.List;

import voldemort.client.ClientConfig;
import voldemort.client.DefaultStoreClient;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClientFactory;
import voldemort.utils.Utils;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;

public class VersionedPartitionStoreWriter {

    public static final short PARTITION_VERSION_NODE_ID = Short.MAX_VALUE;
    private String _storeName;
    private String _bootstrapUrl;
    private DefaultStoreClient<Object, Object> _client;

    public VersionedPartitionStoreWriter(String storeName, String bootstrapUrl) {
        _storeName = storeName;
        _bootstrapUrl = bootstrapUrl;

        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setEnableLazy(false)
                                                                                    .setBootstrapUrls(_bootstrapUrl));

        try {
            _client = (DefaultStoreClient<Object, Object>) factory.getStoreClient(_storeName);
        } catch(Exception e) {
            Utils.croak("Could not connect to server: " + e.getMessage());
        }
    }

    public Version put(String key, String value) {
        return _client.put(key, value);
    }

    public static void main(String[] args) throws Exception {
        if(args.length < 2 || args.length > 5)
            Utils.croak("USAGE: java VersionedPartitionWriter store_name bootstrap_url write_interval total_count");

        String storeName = args[0];
        String bootstrapUrl = args[1];
        long writeInterval = Long.parseLong(args[2]);
        int totalCount = Integer.parseInt(args[3]);
        String key = storeName + "_P1";
        List<ClockEntry> entries = new ArrayList<ClockEntry>();
        entries.add(new ClockEntry(PARTITION_VERSION_NODE_ID, 1));
        VectorClock version = new VectorClock(entries, System.currentTimeMillis());

        VersionedPartitionStoreWriter client = new VersionedPartitionStoreWriter(storeName,
                                                                                 bootstrapUrl);

        int count = 0;
        while(count < totalCount) {
            client.put(key, new String(version.toBytes()));
            version.incrementVersion(PARTITION_VERSION_NODE_ID, System.currentTimeMillis());
            count++;
            Thread.sleep(writeInterval);
        }
    }
}
