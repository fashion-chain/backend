package org.fok.core.db.bdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.fok.core.dbapi.ODBException;
import org.fok.core.dbapi.ODBSupport;
import org.fok.core.dbmodel.Entity.SecondaryValue;
import org.fok.tools.bytes.BytesComparisons;
import org.fok.tools.bytes.BytesHashMap;

import com.google.protobuf.ByteString;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import onight.tfw.ojpa.api.DomainDaoSupport;
import onight.tfw.ojpa.api.ServiceSpec;

@Slf4j
@Data
public class OBDBImpl implements ODBSupport, DomainDaoSupport {
	String domainName = "";
	private Database dbs;
	private SecondaryDatabase sdb;

	private boolean autoSync = true;
	private TransactionConfig txnConfig;
	AtomicInteger relayWriteCounter = new AtomicInteger(0);

	public OBDBImpl(String domain, Database dbs) {
		this.domainName = domain;
		this.dbs = dbs;
		txnConfig = new TransactionConfig();
		txnConfig.setReadCommitted(true);
	}

	public OBDBImpl(String domain, Database dbs, Database sdbs) {
		this.domainName = domain;
		this.dbs = dbs;
		this.sdb = (SecondaryDatabase) sdbs;
		txnConfig = new TransactionConfig();
		txnConfig.setReadCommitted(true);
	}

	@Override
	public DomainDaoSupport getDaosupport() {

		if (dbs != null) {
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

	public void commitTxn(Transaction txn) {
		txn.commit();
	}

	public void close() {
		if (dbs != null) {
			dbs.close();
		}
		if (sdb != null) {
			sdb.close();
		}
	}

	public void sync() {
		if (!this.dbs.getEnvironment().getConfig().getTransactional()) {
			dbs.sync();
		}
	}

	@Override
	public ServiceSpec getServiceSpec() {
		return new ServiceSpec("obdb");
	}

	@Override
	public void setDaosupport(DomainDaoSupport dao) {
		log.trace("setDaosupport::dao=" + dao);
	}

	@Override
	public Future<byte[][]> batchDelete(List<byte[]> keys) throws ODBException {
		Transaction txn = null;
		try {
			// need TransactionConfig?
			txn = this.dbs.getEnvironment().beginTransaction(null, this.txnConfig);
			for (byte[] key : keys) {
				this.dbs.delete(txn, new DatabaseEntry(key));
			}
			commitTxn(txn);
		} catch (Exception ex) {
			if (txn != null) {
				txn.abort();
				txn = null;
			}
			log.error("error on batchDelete, size::" + keys.size(), ex);
		}

		return ConcurrentUtils.constantFuture(null);
	}

	@Override
	public Future<byte[][]> batchPuts(List<byte[]> keys, List<byte[]> values) throws ODBException {
		Transaction txn = null;
		List<byte[]> ret = new ArrayList<>();

		try {
			txn = this.dbs.getEnvironment().beginTransaction(null, this.txnConfig);
			for (int i = 0; i < keys.size(); i++) {
				OperationStatus os = this.dbs.put(txn, new DatabaseEntry(keys.get(i)),
						new DatabaseEntry(values.get(i)));
				if (os == OperationStatus.SUCCESS) {
					ret.add(values.get(i));
				} else {

				}
			}
			commitTxn(txn);
		} catch (Exception ex) {
			if (txn != null) {
				txn.abort();
				txn = null;
			}
			log.error("error on batchPuts, size::" + keys.size(), ex);
		}
		return ConcurrentUtils.constantFuture(ret.toArray(new byte[][] {}));
	}

	@Override
	public Future<byte[]> delete(byte[] key) throws ODBException {
		dbs.delete(null, new DatabaseEntry(key));
		return ConcurrentUtils.constantFuture(null);
	}

	@Override
	public Future<BytesHashMap<byte[]>> deleteBySecondKey(byte[] secondaryName, List<byte[]> keys) throws ODBException {
		try {
			BytesHashMap<byte[]> list = listBySecondKey(secondaryName).get();
			List<byte[]> existsKeys = new ArrayList<byte[]>();
			for (byte[] sKey : list.keySet()) {
				for (byte[] oKey : keys) {
					if (BytesComparisons.equal(oKey, sKey)) {
						existsKeys.add(oKey);
					}
				}
			}
			batchDelete(existsKeys);
		} catch (InterruptedException e) {
			log.error("error on deleteBySecondKey", e);
		} catch (ExecutionException e) {
			log.error("error on deleteBySecondKey", e);
		}

		return null;
	}

	@Override
	public Future<byte[]> get(byte[] key) throws ODBException {
		DatabaseEntry searchEntry = new DatabaseEntry();
		dbs.get(null, new DatabaseEntry(key), searchEntry, LockMode.DEFAULT);

		if (searchEntry.getData() == null) {
			return ConcurrentUtils.constantFuture(null);
		} else {
			byte[] v = searchEntry.getData();
			return ConcurrentUtils.constantFuture(v);
		}
	}

	@Override
	public Future<byte[][]> list(List<byte[]> keys) throws ODBException {
		List<byte[]> list = new ArrayList<byte[]>();
		for (byte[] key : keys) {
			DatabaseEntry searchEntry = new DatabaseEntry();
			dbs.get(null, new DatabaseEntry(key), searchEntry, LockMode.DEFAULT);
			if (searchEntry.getData() == null) {

			} else {
				list.add(searchEntry.getData());
			}
		}
		return ConcurrentUtils.constantFuture(list.toArray(new byte[][] {}));
	}

	@Override
	public Future<BytesHashMap<byte[]>> listBySecondKey(byte[] secondaryName) throws ODBException {
		if (sdb != null) {
			SecondaryCursor mySecCursor = null;
			try {
				DatabaseEntry secondaryKey = new DatabaseEntry(secondaryName);
				DatabaseEntry foundKey = new DatabaseEntry();
				DatabaseEntry foundData = new DatabaseEntry();

				CursorConfig oCursorConfig = new CursorConfig();
				oCursorConfig.setReadCommitted(true);
				mySecCursor = sdb.openCursor(null, oCursorConfig);

				OperationStatus retVal = mySecCursor.getSearchKey(secondaryKey, foundKey, foundData, LockMode.DEFAULT);
				BytesHashMap<byte[]> ret = new BytesHashMap<>();

				while (retVal == OperationStatus.SUCCESS) {
					SecondaryValue sv = SecondaryValue.parseFrom(foundData.getData());
					ret.put(foundKey.getData(), sv.getData().toByteArray());
					retVal = mySecCursor.getNextDup(secondaryKey, foundKey, foundData, LockMode.DEFAULT);
				}

				return ConcurrentUtils.constantFuture(ret);
			} catch (Exception e) {
				log.error("ODBError", e);
				return ConcurrentUtils.constantFuture(null);
			} finally {
				if (mySecCursor != null) {
					mySecCursor.close();
				}
			}
		} else {
			return ConcurrentUtils.constantFuture(null);
		}
	}

	@Override
	public Future<byte[]> put(byte[] key, byte[] v) throws ODBException {
		DatabaseEntry keyValue = new DatabaseEntry(key);
		DatabaseEntry dataValue = new DatabaseEntry(v);
		OperationStatus os = dbs.put(null, keyValue, dataValue);
		if (os == OperationStatus.SUCCESS) {
			return ConcurrentUtils.constantFuture(v);
		} else {
			return ConcurrentUtils.constantFuture(null);
		}
	}

	@Override
	public Future<byte[]> put(byte[] key, byte[] secondaryKey, byte[] v) throws ODBException {
		SecondaryValue.Builder sv = SecondaryValue.newBuilder();
		sv.setData(ByteString.copyFrom(v));
		sv.setSecondaryKey(ByteString.copyFrom(secondaryKey));
		return put(key, sv.build().toByteArray());
	}

	@Override
	public Future<byte[]> putIfNotExist(byte[] key, byte[] v) throws ODBException {
		DatabaseEntry keyValue = new DatabaseEntry(key);
		DatabaseEntry dataValue = new DatabaseEntry(v);
		OperationStatus opstatus = dbs.putNoOverwrite(null, keyValue, dataValue);
		if (opstatus == OperationStatus.KEYEXIST) {
			return ConcurrentUtils.constantFuture(null);
		} else {
			return ConcurrentUtils.constantFuture(v);
		}
	}

}
