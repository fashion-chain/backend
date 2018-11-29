package org.fok.core.db.bdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.fok.core.dbapi.ODBException;
import org.fok.tools.bytes.BytesHashMap;

import com.sleepycat.je.Database;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeferOBDBImpl extends OBDBImpl implements Runnable {

	public BytesHashMap<byte[]> memoryMap = new BytesHashMap<>();
	int maxSize = 100;
	long delayWriteMS = 200;

	public DeferOBDBImpl(int maxSize, long delayWriteMS, String domain, Database dbs, Database sdbs) {
		super(domain, dbs, sdbs);
		this.maxSize = maxSize;
		this.delayWriteMS = delayWriteMS;
	}

	public DeferOBDBImpl(int maxSize, long delayWriteMS, String domain, Database dbs) {
		super(domain, dbs);
		this.maxSize = maxSize;
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

	public synchronized void deferSync() {
		if (memoryMap.size() > 0
				&& (memoryMap.size() >= maxSize || System.currentTimeMillis() - lastSyncTime > delayWriteMS)) {
			List<byte[]> keys = new ArrayList<>();
			List<byte[]> values = new ArrayList<>();
			for (Map.Entry<byte[], byte[]> entry : memoryMap.entrySet()) {
				keys.add(entry.getKey());
				values.add(entry.getValue());
			}
			super.batchPuts(keys, values);
			memoryMap.clear();
			lastSyncTime = System.currentTimeMillis();
		}
	}

	@Override
	public void close() {
		deferSync();
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
		return super.get(key);
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
	}

}
