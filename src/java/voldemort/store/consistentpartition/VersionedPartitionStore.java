package voldemort.store.consistentpartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.StringSerializer;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class VersionedPartitionStore extends DelegatingStore<ByteArray, byte[], byte[]> {

    private static final Logger logger = Logger.getLogger(VersionedPartitionStore.class);
    private static final Hex hexCodec = new Hex();

    private final Store<ByteArray, byte[], byte[]> _partitionVersionStore;
    private final RoutingStrategy _routingStrategy;
    private final MetadataStore _metadata;
    private final StoreDefinition _storeDef;

    public VersionedPartitionStore(Store<ByteArray, byte[], byte[]> innerStore,
                                   Store<ByteArray, byte[], byte[]> partitionVersionStore,
                                   MetadataStore metadata,
                                   StoreDefinition storeDef) {
        super(innerStore);
        _partitionVersionStore = partitionVersionStore;
        _routingStrategy = metadata.getRoutingStrategy(storeDef.getName());
        _metadata = metadata;
        _storeDef = storeDef;

        logger.info("VersionedPartitionStore:" + " innerStore=" + getInnerStore()
                    + " partitionVersionStore=" + _partitionVersionStore + " metadata=" + _metadata
                    + " storeDef=" + _storeDef);

    }

    // TODO: make sure partition version has only one value at any time.
    // TODO: once partition version has its own store, make the keys more
    // compact.
    private ByteArray getPartitionKey(ByteArray entryKey) {
        Integer partitionId = _routingStrategy.getPartitionList(entryKey.get()).get(0);
        String key = _storeDef.getName() + "_P" + partitionId;
        // logger.debug("leigao4 Partition key=" + key + " used for key=" + new
        // String(entryKey.get()));
        return new ByteArray(key.getBytes());
    }

    private VectorClock loadPartitionVersion(ByteArray partitionKey) {
        long maxVersion = 0;
        long timestamp = 0;
        StringSerializer serializer = new StringSerializer();
        List<Versioned<byte[]>> versionedList = _partitionVersionStore.get(partitionKey, null);
        for(Iterator<Versioned<byte[]>> it = versionedList.iterator(); it.hasNext();) {
            String stringVersion = serializer.toObject(it.next().getValue());
            VectorClock tmpVersion = new VectorClock(stringVersion.getBytes());
            logger.debug("leigao: " + tmpVersion);
            long localMax = tmpVersion.getMaxVersion();
            if(localMax > maxVersion) {
                maxVersion = localMax;
                timestamp = tmpVersion.getTimestamp();
            }
        }

        logger.debug("leigao3: " + maxVersion);

        if(0 < maxVersion) {
            // this is a hack: Short.MAX_VALUE is where the partition version is
            // stored for now
            ClockEntry entry = new ClockEntry(Short.MAX_VALUE, maxVersion);
            List<ClockEntry> entryList = new ArrayList<ClockEntry>(1);
            entryList.add(entry);
            return new VectorClock(entryList, timestamp);
        } else {
            return null;
        }
    }

    // TODO: when loadPartitionVersion returns null, we don't put null in cache.
    // As a result,
    // subsequent get can't benefit from the cache. Change the cache to use a
    // map that distinguish
    // null value vs. non-existing-entry
    private Version getPartitionVersionWithCache(ByteArray key,
                                                 Map<ByteArray, Version> partitionVersionCache) {
        ByteArray partitionKey = getPartitionKey(key);
        Version partitionVersion = null;

        // if we are using a cache for partition versions, get it from cache
        if(null != partitionVersionCache) {
            partitionVersion = partitionVersionCache.get(partitionKey);
        }

        // load from partition version store if not available
        if(null == partitionVersion) {
            partitionVersion = loadPartitionVersion(partitionKey);

            // put version in cache if cache is in use
            if(null != partitionVersionCache) {
                partitionVersionCache.put(partitionKey, partitionVersion);
            }
        }

        return partitionVersion;
    }

    private void mergeVersionWithPartitionVersion(ByteArray key,
                                                  List<Version> versionList,
                                                  Map<ByteArray, Version> partitionVersionCache) {

        Version partitionVersion = getPartitionVersionWithCache(key, partitionVersionCache);
        if(null != partitionVersion) {
            logger.debug("leigao7: " + partitionVersion);
            versionList.add(partitionVersion);
        }
    }

    private void mergeValueWithPartitionVersion(ByteArray key,
                                                List<Versioned<byte[]>> versionedList,
                                                Map<ByteArray, Version> partitionVersionCache) {

        Version partitionVersion = getPartitionVersionWithCache(key, partitionVersionCache);
        if(null != partitionVersion) {
            Versioned<byte[]> versioned = new Versioned<byte[]>(new byte[1], partitionVersion);
            versionedList.add(versioned);
        }
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        boolean deleted = false;

        ByteArray partitionKey = getPartitionKey(key);
        VectorClock partitionVersion = loadPartitionVersion(partitionKey);
        if(null != partitionVersion) {
            partitionVersion = ((VectorClock) version).clone()
                                                      .setAllEntries(partitionVersion.getMaxVersion());
        }

        // if partitionVersion is 'before' the version of delete, go through
        if(null == partitionVersion || Occured.BEFORE == partitionVersion.compare(version)) {
            deleted = getInnerStore().delete(key, version);
        }

        return deleted;
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Map<ByteArray, Version> partitionVersionCache = new HashMap<ByteArray, Version>();
        Map<ByteArray, List<Versioned<byte[]>>> values = getInnerStore().getAll(keys, transforms);

        for(Entry<ByteArray, List<Versioned<byte[]>>> entry: values.entrySet()) {
            List<Versioned<byte[]>> versionList = entry.getValue();
            mergeValueWithPartitionVersion(entry.getKey(), versionList, partitionVersionCache);
        }

        return values;
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);

        ByteArray partitionKey = getPartitionKey(key);
        VectorClock partitionVersion = loadPartitionVersion(partitionKey);
        if(null != partitionVersion) {
            partitionVersion = ((VectorClock) value.getVersion()).clone()
                                                                 .setAllEntries(partitionVersion.getMaxVersion());
        }
        logger.debug("leigao: partitionVersion=" + partitionVersion);
        logger.debug("leigao: putVersion=" + value.getVersion());
        // if value is 'before' partition version, throw exception
        if(null != partitionVersion
           && Occured.BEFORE == value.getVersion().compare(partitionVersion)) {
            throw new ObsoleteVersionException("Key "
                                               + new String(hexCodec.encode(key.get()))
                                               + " "
                                               + value.getVersion().toString()
                                               + " is obsolete, it is no greater than the current PARTITION version of "
                                               + partitionVersion + ".");

        }

        getInnerStore().put(key, value, transforms);
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        StoreUtils.assertValidKey(key);

        List<Versioned<byte[]>> versionedList = getInnerStore().get(key, transforms);
        mergeValueWithPartitionVersion(key, versionedList, null);
        return versionedList;
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        StoreUtils.assertValidKey(key);

        List<Version> versionList = getInnerStore().getVersions(key);
        mergeVersionWithPartitionVersion(key, versionList, null);
        return versionList;
    }
}
