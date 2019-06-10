package org.fok.core.db.bdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.fok.core.dbapi.ODBException;
import org.fok.core.dbapi.ODBSupport;
import org.fok.tools.bytes.BytesHashMap;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import onight.tfw.ojpa.api.DomainDaoSupport;
import onight.tfw.ojpa.api.ServiceSpec;

@Slf4j
@Data
public class SlicerOBDBImpl implements ODBSupport, DomainDaoSupport {
	String domainName = "";

	OBDBImpl odbs[];

	int sliceCount = 1;

	ScheduledExecutorService exec;

	public SlicerOBDBImpl(String domain, OBDBImpl odbs[], ScheduledExecutorService exec) {
		this.odbs = odbs;
		this.domainName = domain;
		this.sliceCount = odbs.length;
		this.exec = exec;
	}

	@Override
	public DomainDaoSupport getDaosupport() {
		if (odbs != null) {
			return this;
		} else {
			return null;
		}
	}

	@Override
	public Class<?> getDomainClazz() {
		return Object.class;
	}

	@Override
	public String getDomainName() {
		return "etcd";
	}

	public void close() {
		for (OBDBImpl odb : odbs) {
			odb.close();
		}
	}

	public void sync() {
		for (OBDBImpl odb : odbs) {
			odb.sync();
		}
	}

	public int getSliceId(byte[] bs) {
		return Math.abs(bs[0]) % sliceCount;
	}

	public int getSliceId(String bs) {
		byte[] bb = bs.getBytes();
		return Math.abs(bb[0]) % sliceCount;
	}

	@Override
	public ServiceSpec getServiceSpec() {
		return new ServiceSpec("obdb");
	}

	@Override
	public void setDaosupport(DomainDaoSupport dao) {
		log.trace("setDaosupport::dao=" + dao);
	}

	public OBDBImpl getDb(byte[] key) {
		return odbs[getSliceId(key)];
	}

	public OBDBImpl getDb(String key) {
		return odbs[getSliceId(key)];
	}

	class SlicePair {
		List<byte[]> keys = new ArrayList<>();
		List<byte[]> values = new ArrayList<>();
		List<byte[]> newvalues = new ArrayList<>();
	}

	public SlicePair[] seperate(List<byte[]> keys, List<byte[]> values) {
		SlicePair[] kvs = new SlicePair[sliceCount];
		for (int i = 0; i < keys.size(); i++) {
			int id = getSliceId(keys.get(i));
			SlicePair sp = kvs[id];
			if (sp == null) {
				sp = new SlicePair();
				kvs[id] = sp;
			}
			sp.keys.add(keys.get(i));
			sp.values.add(values.get(i));

		}
		return kvs;
	}

	public SlicePair[] seperate(List<byte[]> keys) {
		SlicePair[] kvs = new SlicePair[sliceCount];
		for (int i = 0; i < keys.size(); i++) {
			int id = getSliceId(keys.get(i));
			SlicePair sp = kvs[id];
			if (sp == null) {
				sp = new SlicePair();
				kvs[id] = sp;
			}
			sp.keys.add(keys.get(i));
		}
		return kvs;
	}

	public SlicePair[] seperate(List<byte[]> keys, List<byte[]> values, List<byte[]> newvalues) {
		SlicePair[] kvs = new SlicePair[sliceCount];
		for (int i = 0; i < keys.size(); i++) {
			int id = getSliceId(keys.get(i));
			SlicePair sp = kvs[id];
			if (sp == null) {
				sp = new SlicePair();
				kvs[id] = sp;
			}
			sp.keys.add(keys.get(i));
			sp.values.add(values.get(i));
			sp.newvalues.add(newvalues.get(i));
		}
		return kvs;
	}

	@AllArgsConstructor
	class BatchPutsRunner implements Runnable {
		OBDBImpl odb;

		List<byte[]> keys;
		List<byte[]> values;
		List<byte[]> resultSet;
		CountDownLatch cdl;

		@Override
		public void run() {
			try {
				Future<byte[][]> f = odb.batchPuts(keys, values);
				if (f != null && f.get() != null) {
					for (byte[] v : f.get()) {
						if (v != null) {
							resultSet.add(v);
						}
					}
				}
			} catch (Exception e) {
			} finally {
				cdl.countDown();
			}
		}
	}

	@Override
	public Future<byte[][]> batchDelete(List<byte[]> keys) throws ODBException {
		SlicePair[] kvs = seperate(keys);
		for (int i = 0; i < sliceCount; i++) {
			if (kvs[i] != null) {
				try {
					byte[][] ret = odbs[i].batchDelete(kvs[i].keys).get();
					return ConcurrentUtils.constantFuture(ret);
				} catch (Exception e) {
					throw new ODBException(e);
				}

			}
		}

		return ConcurrentUtils.constantFuture(null);
	}

	@Override
	public Future<byte[][]> batchPuts(List<byte[]> keys, List<byte[]> values) throws ODBException {
		SlicePair[] kvs = seperate(keys, values);
		CountDownLatch cdl = new CountDownLatch(sliceCount);
		List<byte[]> ret = new ArrayList<>();

		for (int i = 0; i < sliceCount; i++) {
			if (kvs[i] != null) {
				if (kvs[i].keys.size() > 1) {
					try {
						BatchPutsRunner oBatchPutsRunner = new BatchPutsRunner(odbs[i], kvs[i].keys, kvs[i].values, ret,
								cdl);
						exec.execute(oBatchPutsRunner);
						oBatchPutsRunner = null;
					} catch (Exception e) {
						throw new ODBException(e);
					}
				} else {
					try {
						Future<byte[]> v = odbs[i].put(kvs[i].keys.get(0), kvs[i].values.get(0));
						if (v != null && v.get() != null) {
							ret.add(v.get());
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					cdl.countDown();
				}
			} else {
				cdl.countDown();
			}
		}
		try {
			cdl.await(30, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			throw new ODBException("Batch put TimeoutException");
		}
		return ConcurrentUtils.constantFuture(ret.toArray(new byte[][] {}));
	}

	@Override
	public Future<byte[]> delete(byte[] key) throws ODBException {
		return getDb(key).delete(key);
	}

	@Override
	public Future<BytesHashMap<byte[]>> deleteBySecondKey(byte[] arg0, List<byte[]> arg1) throws ODBException {
		throw new RuntimeException("Not supported");
	}

	@Override
	public Future<byte[]> get(byte[] key) throws ODBException {
		return getDb(key).get(key);
	}

	@Override
	public Future<byte[][]> list(List<byte[]> keys) throws ODBException {
		SlicePair[] kvs = seperate(keys);
		List<byte[]> list = new ArrayList<>();
		for (int i = 0; i < sliceCount; i++) {
			if (kvs[i] != null) {
				try {
					byte[][] ret = odbs[i].list(kvs[i].keys).get();
					list.addAll(Arrays.asList(ret));
				} catch (Exception e) {
					throw new ODBException(e);
				}

			}
		}
		return ConcurrentUtils.constantFuture(list.toArray(new byte[][] {}));
	}

	@Override
	public Future<BytesHashMap<byte[]>> listBySecondKey(byte[] secondaryName) throws ODBException {
		BytesHashMap<byte[]> ret = new BytesHashMap<>();
		for (int i = 0; i < sliceCount; i++) {
			try {
				BytesHashMap<byte[]> subret = odbs[i].listBySecondKey(secondaryName).get();
				for (byte[] key : subret.keySet()) {
					ret.put(key, subret.get(key));
				}
			} catch (Exception e) {
				throw new ODBException(e);
			}
		}
		return ConcurrentUtils.constantFuture(ret);
	}

	@Override
	public Future<byte[]> put(byte[] key, byte[] v) throws ODBException {
		return getDb(key).put(key, v);
	}

	@Override
	public Future<byte[]> put(byte[] key, byte[] secondaryKey, byte[] v) throws ODBException {
		return getDb(key).put(key, secondaryKey, v);
	}

	@Override
	public Future<byte[]> putIfNotExist(byte[] key, byte[] v) throws ODBException {
		return getDb(key).putIfNotExist(key, v);
	}

}
