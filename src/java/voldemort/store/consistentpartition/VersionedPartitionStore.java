package voldemort.store.consistentpartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.routing.RoutingStrategy;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
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

    public VersionedPartitionStore(Store<ByteArray, byte[], byte[]> innerStore,
                                          Store<ByteArray, byte[], byte[]> partitionVersionStore,
                                          RoutingStrategy routingStrategy) {
        super(innerStore);
        _partitionVersionStore = partitionVersionStore;
        _routingStrategy = routingStrategy;
    }

    // TODO: make sure partition version has only one value at any time.
    private ByteArray getPartitionKey(ByteArray entryKey) {
        Integer partitionId = _routingStrategy.getPartitionList(entryKey.get()).get(0);
        return new ByteArray(partitionId.byteValue());
    }

    // TODO: make sure partition version has only one value at any time.
    private Version loadPartitionVersion(ByteArray partitionKey) {
        return _partitionVersionStore.getVersions(partitionKey).get(0);
    }

    private VectorClock mergeWithPartitionVersion(ByteArray key,
                                                  VectorClock version,
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

            // put verion in cache if cache is in use
            if(null != partitionVersionCache) {
                partitionVersionCache.put(partitionKey, partitionVersion);
            }
        }

        return version.merge((VectorClock) partitionVersion);
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        boolean deleted = false;

        ByteArray partitionKey = getPartitionKey(key);
        Version partitionVersion = loadPartitionVersion(partitionKey);

        // if partitionVersion is 'before' the version of delete, go through
        if(Occured.BEFORE == partitionVersion.compare(version)) {
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
            // TODO: figure out how to deal with enties w/ multiple conflicting
            // versions; for now we only work with the first one (which is
            // incorrect).
            List<Versioned<byte[]>> versionList = entry.getValue();
            Versioned<byte[]> versioned = versionList.get(0);
            Version mergedVersion = mergeWithPartitionVersion(entry.getKey(),
                                                              (VectorClock) versioned.getVersion(),
                                                              partitionVersionCache);
            versionList.set(0, new Versioned<byte[]>(versioned.getValue(), mergedVersion));
        }

        return values;
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);

        ByteArray partitionKey = getPartitionKey(key);
        Version partitionVersion = loadPartitionVersion(partitionKey);

        // if value is 'before' partition version, throw exception
        if(Occured.BEFORE == value.getVersion().compare(partitionVersion)) {
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

        // TODO: figure out how to deal with enties w/ multiple conflicting
        // versions; for now we only work with the first one (which is
        // incorrect).
        List<Versioned<byte[]>> versionedList = getInnerStore().get(key, transforms);
        Versioned<byte[]> versioned = versionedList.get(0);
        Version mergedVersion = mergeWithPartitionVersion(key,
                                                          (VectorClock) versioned.getVersion(),
                                                          null);
        versionedList.set(0, new Versioned<byte[]>(versioned.getValue(), mergedVersion));
        return versionedList;
    }
}
