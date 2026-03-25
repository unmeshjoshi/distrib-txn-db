package kv;

import clock.HybridTimestamp;

import java.util.Map;
import java.util.Optional;

/**
 * Standard interface strictly mapping logic through physical byte primitives.
 */
public interface MVCCStore {
    boolean put(MVCCKey key, byte[] value);
    
    boolean putBatch(Map<MVCCKey, byte[]> mutations);
    
    Optional<byte[]> getAsOf(MVCCKey searchKey);

    Optional<byte[]> getLatest(byte[] key);

    Map<HybridTimestamp, byte[]> getVersionsUpTo(byte[] key, HybridTimestamp asOfTime);

    Map<byte[], byte[]> scanPrefixAsOf(byte[] prefix, HybridTimestamp asOfTime);
    
    void tick();
    
    void close();
}
