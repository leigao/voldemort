/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.versioned;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

/**
 * A Store that uses a InconsistencyResolver to eliminate some duplicates.
 * 
 * 
 */
/*
 * Note that unlike get and getAll, getVersions is not overridden so the
 * versions are not passed through the inconsistency resolver.
 */
public class InconsistencyResolvingStore<K, V, T> extends DelegatingStore<K, V, T> {

    private final InconsistencyResolver<Versioned<V>> resolver;

    public InconsistencyResolvingStore(Store<K, V, T> innerStore,
                                       InconsistencyResolver<Versioned<V>> resolver) {
        super(innerStore);
        this.resolver = resolver;
    }

    private List<Versioned<V>> mergeVersionedWithPartitionVersion(List<Versioned<V>> versionedList,
                                                                  VectorClock partitionVersion) {
        List<Versioned<V>> mergedVersionedList = new ArrayList<Versioned<V>>(versionedList.size());

        for(Iterator<Versioned<V>> it = versionedList.iterator(); it.hasNext();) {
            Versioned<V> originalVersioned = it.next();
            VectorClock version = (VectorClock) originalVersioned.getVersion();
            Version mergedVersion = version.mergeAllEntries(partitionVersion.getMaxVersion());
            mergedVersionedList.add(new Versioned<V>(originalVersioned.getValue(), mergedVersion));
        }
        return mergedVersionedList;
    }

    private Version removePartitionVersioned(List<Versioned<V>> versionedList) {
        VectorClock partitionVersion = null;
        int lastEntryIndex = versionedList.size() - 1;

        if(lastEntryIndex >= 0) {
            partitionVersion = (VectorClock) versionedList.get(lastEntryIndex).getVersion();
            if(partitionVersion.getEntries().get(0).getNodeId() == Short.MAX_VALUE) {
                partitionVersion = (VectorClock) versionedList.remove(lastEntryIndex).getVersion();
            }
        }
        return partitionVersion;
    }

    private List<Version> mergeVersionWithPartitionVersion(List<Version> versionList,
                                                           VectorClock partitionVersion) {
        List<Version> mergedVersionList = new ArrayList<Version>(versionList.size());

        for(Iterator<Version> it = versionList.iterator(); it.hasNext();) {
            VectorClock originalVersion = (VectorClock) it.next();
            Version mergedVersion = originalVersion.mergeAllEntries(partitionVersion.getMaxVersion());
            mergedVersionList.add(mergedVersion);
        }
        return mergedVersionList;
    }

    private VectorClock removePartitionVersion(List<Version> versionList) {
        VectorClock partitionVersion = null;
        int lastEntryIndex = versionList.size() - 1;

        if(lastEntryIndex >= 0) {
            partitionVersion = (VectorClock) versionList.get(lastEntryIndex);
            if(partitionVersion.getEntries().get(0).getNodeId() == Short.MAX_VALUE) {
                partitionVersion = (VectorClock) versionList.remove(lastEntryIndex);
            }
        }
        return partitionVersion;
    }

    @Override
    public List<Versioned<V>> get(K key, T transforms) throws VoldemortException {
        VectorClock partitionVersion = null;
        List<Versioned<V>> versionedList = super.get(key, transforms);

        // remove partition version from the list if exist before passing it to
        // resolver
        if(!versionedList.isEmpty()) {
            partitionVersion = (VectorClock) removePartitionVersioned(versionedList);
        }

        // resolve conflicts
        versionedList = resolver.resolveConflicts(versionedList);

        // now, merge with partition version
        if(null != partitionVersion && !versionedList.isEmpty()) {
            versionedList = mergeVersionedWithPartitionVersion(versionedList, partitionVersion);
        }

        return versionedList;
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        Map<K, List<Versioned<V>>> m = super.getAll(keys, transforms);
        for(Map.Entry<K, List<Versioned<V>>> entry: m.entrySet()) {
            VectorClock partitionVersion = null;
            List<Versioned<V>> versionedList = entry.getValue();

            // remove partition version from the list if exist before passing it
            // to resolver
            if(!versionedList.isEmpty()) {
                partitionVersion = (VectorClock) removePartitionVersioned(versionedList);
            }

            // resolve conflicts
            versionedList = resolver.resolveConflicts(versionedList);

            // now, merge with partition version
            if(null != partitionVersion && !versionedList.isEmpty()) {
                versionedList = mergeVersionedWithPartitionVersion(versionedList, partitionVersion);
            }

            m.put(entry.getKey(), versionedList);
        }
        return m;
    }

    @Override
    public List<Version> getVersions(K key) {
        List<Version> versionList = super.getVersions(key);

        // remove the partition version if exist
        VectorClock partitionVersion = removePartitionVersion(versionList);

        // need dedup or remove older version if possible
        if(versionList.size() > 1) {
            boolean conflictFound = false;
            Version maxVersion = versionList.get(0);
            for(Iterator<Version> it = versionList.iterator(); it.hasNext();) {
                Version currentVersion = it.next();
                Occured occured = currentVersion.compare(maxVersion);
                if(Occured.CONCURRENTLY == occured) {
                    conflictFound = true;
                    break;
                } else if(Occured.AFTER == occured) {
                    maxVersion = currentVersion;
                } else {
                    // sliently discard if it's 'BEFORE'
                }
            }

            // only modify versionList if no conflict found
            if(!conflictFound) {
                versionList = Lists.newArrayList();
                versionList.add(((VectorClock) maxVersion).clone());
            }
        }

        // merge with partition version
        if(null != partitionVersion) {
            if(versionList.isEmpty()) {
                // this is temporary hack to populate a version with first 10
                // EntryClock populated.
                // TODO: remove this hack by change 10 to replication factor of
                // this store.
                List<ClockEntry> entryList = Lists.newArrayList();
                for(short i = 0; i < 10; i++) {
                    entryList.add(new ClockEntry(i, partitionVersion.getMaxVersion()));
                }
                versionList.add(new VectorClock(entryList, partitionVersion.getTimestamp()));
            } else if(1 == versionList.size()) {
                versionList = mergeVersionWithPartitionVersion(versionList, partitionVersion);
            } else {
                // ignore since concurrent versions will be discarded anyways.
            }
        }

        return versionList;
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        if(capability == StoreCapabilityType.INCONSISTENCY_RESOLVER)
            return this.resolver;
        else
            return super.getCapability(capability);
    }

}
