package org.fok.core.dbapi;

import java.util.List;
import java.util.concurrent.Future;

import org.fok.tools.bytes.BytesHashMap;

import lombok.Data;
import onight.tfw.ojpa.api.DomainDaoSupport;
import onight.tfw.ojpa.api.ServiceSpec;

@Data
public class ODBDao<K, V> implements ODBSupport<K, V> {

	protected ServiceSpec serviceSpec;
	protected ODBSupport<K, V> daosupport;
	protected String domainName;

	public ODBDao(ServiceSpec serviceSpec) {
		this.serviceSpec = serviceSpec;
	}

	// public

	@Override
	public Class<?> getDomainClazz() {
		return null;
	}

	@Override
	public void setDaosupport(DomainDaoSupport dds) {
		this.daosupport = (ODBSupport<K, V>) dds;
	}

	@Override
	public Future<V> get(K key) throws ODBException {
		return daosupport.get(key);
	}

	@Override
	public Future<V[]> list(List<K> key) throws ODBException {
		return daosupport.list(key);
	}

	@Override
	public Future<BytesHashMap<byte[]>> listBySecondKey(K secondKey) throws ODBException {
		return daosupport.listBySecondKey(secondKey);
	}

	@Override
	public Future<V> put(K key, V value) throws ODBException {
		return daosupport.put(key, value);
	}

	@Override
	public Future<V> put(K key, K secondaryKey, V value) throws ODBException {
		return daosupport.put(key, secondaryKey, value);
	}

	@Override
	public Future<V[]> batchPuts(List<K> key, List<V> value) throws ODBException {
		return daosupport.batchPuts(key, value);
	}

	@Override
	public Future<V> putIfNotExist(K key, V value) throws ODBException {
		return daosupport.putIfNotExist(key, value);
	}

	@Override
	public Future<V> delete(K key) throws ODBException {
		return daosupport.delete(key);
	}

	@Override
	public Future<V[]> batchDelete(List<K> key) throws ODBException {
		return daosupport.batchDelete(key);
	}

	@Override
	public Future<BytesHashMap<byte[]>> deleteBySecondKey(K secondKey, List<K> keys) throws ODBException {
		return daosupport.deleteBySecondKey(secondKey, keys);
	}

	@Override
	public void sync() throws ODBException {
		daosupport.sync();
	}
}
