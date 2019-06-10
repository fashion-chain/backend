package org.csc.backend.bc_leveldb.provider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.brewchain.core.dbapi.ODBException;
import org.csc.backend.bc_leveldb.api.LDatabase;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeferOBDBImpl extends OLevelDBImpl implements Runnable {

	public ConcurrentHashMap<byte[], byte[]> memoryMap = new ConcurrentHashMap<>();
	public ConcurrentHashMap<byte[], byte[]> l2CacheMap = new ConcurrentHashMap<>();
	int maxSize = 100;
	int maxL2CacheSize = 1000; // 1000
	long delayWriteMS = 200;
	AtomicBoolean dbsyncing = new AtomicBoolean(false);

	public DeferOBDBImpl(int maxSize, int maxL2CacheSize, long delayWriteMS, String domain, LDatabase dbs,
			LDatabase sdbs) {
		super(domain, dbs, sdbs);
		this.maxSize = maxSize;
		this.maxL2CacheSize = maxL2CacheSize;
		this.delayWriteMS = delayWriteMS;
	}

	public DeferOBDBImpl(int maxSize, int maxL2CacheSize, long delayWriteMS, String domain, LDatabase dbs) {
		super(domain, dbs);
		this.maxSize = maxSize;
		this.maxL2CacheSize = maxL2CacheSize;
		this.delayWriteMS = delayWriteMS;
	}

	@Override
	public synchronized Future<byte[][]> batchPuts(List<byte[]> keys, List<byte[]> values) throws ODBException {
		for (int i = 0; i < keys.size(); i++) {
			memoryMap.put(keys.get(i), values.get(i));
		}
		deferSync();
		return ConcurrentUtils.constantFuture(null);
	}

	long lastSyncTime = 0;
	long lastL2SyncTime = 0;

	public synchronized void deferSync() {
		Thread.currentThread().setName("db-defersync-" + domainName);
		if (memoryMap.size() > 0
				&& (memoryMap.size() >= maxSize || System.currentTimeMillis() - lastSyncTime > delayWriteMS)) {
			// OKey[] keys = new OKey[memoryMap.size()];
			// OValue[] values = new OValue[memoryMap.size()];
			// int cc = 0;
			// for (Map.Entry<OKey, OValue> entry : memoryMap.entrySet()) {
			// keys[cc] = entry.getKey();
			// values[cc] = entry.getValue();
			// cc++;
			// }
			// super.batchPuts(keys, values);
			if (dbsyncing.compareAndSet(false, true)) {
				l2CacheMap.putAll(memoryMap);
				memoryMap.clear();
				lastSyncTime = System.currentTimeMillis();
				dbsyncing.set(false);
			}
		}
		Thread.currentThread().setName("dbpools");
	}

	public void dbCacheSync() {
		if (l2CacheMap.size() == 0) {
			return;
		}
		Thread.currentThread().setName("db-L2cachesync-" + domainName);
		while (!dbsyncing.compareAndSet(false, true)) {
			try {
				Thread.sleep(1);
			} catch (Exception e) {
			}
		}
		;
		try {
			List<byte[]> keys = new ArrayList<>();
			List<byte[]> values = new ArrayList<>();

			for (Map.Entry<byte[], byte[]> entry : l2CacheMap.entrySet()) {
				keys.add(entry.getKey());
				values.add(entry.getValue());
			}
			super.batchPuts(keys, values);
			l2CacheMap.clear();
			lastL2SyncTime = System.currentTimeMillis();
		} catch (Throwable t) {
			log.error("l2 dbCacheSync error:" + domainName, t);
		} finally {
			dbsyncing.compareAndSet(true, false);
			Thread.currentThread().setName("db-L2cachesync-" + domainName);
		}
	}

	@Override
	public void close() {
		deferSync();
		dbCacheSync();
		super.close();
	}

	@Override
	public synchronized Future<byte[]> put(byte[] key, byte[] v) throws ODBException {
		memoryMap.put(key, v);
		deferSync();
		return ConcurrentUtils.constantFuture(v);
	}

	@Override
	public Future<byte[]> get(byte[] key) throws ODBException {
		byte[] ov = memoryMap.get(key);
		if (ov != null)
			return ConcurrentUtils.constantFuture(ov);
		ov = l2CacheMap.get(key);
		if (ov != null)
			return ConcurrentUtils.constantFuture(ov);

		return super.get(key);
	}

	@Override
	public synchronized Future<byte[]> delete(byte[] key) throws ODBException {
		memoryMap.remove(key);
		// l2CacheMap.remove(key);
		return super.delete(key);
	}

	@Override
	public synchronized Future<byte[][]> batchDelete(List<byte[]> keys) throws ODBException {
		for (byte[] key : keys) {
			memoryMap.remove(key);
			// l2CacheMap.remove(key);
		}
		return super.batchDelete(keys);
	}

	public synchronized Future<byte[][]> list(List<byte[]> keys) throws ODBException {
		List<byte[]> list = new ArrayList<>();
		for (byte[] key : keys) {
			Future<byte[]> ov = get(key);
			try {
				if (ov != null && ov.get() != null) {
					list.add(ov.get());
				}
			} catch (Exception e) {

			}
		}
		return ConcurrentUtils.constantFuture(list.toArray(new byte[][] {}));

	}

	@Override
	public void run() {
		deferSync();
		dbCacheSync();

	}

}
