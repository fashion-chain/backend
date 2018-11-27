package org.fok.core.dbapi;

import java.util.List;
import java.util.concurrent.Future;

import org.fok.tools.bytes.BytesHashMap;

import onight.tfw.ojpa.api.DomainDaoSupport;

/**
 * add support for odb -- osgi database.
 * 
 * @author brew
 *
 */
public interface ODBSupport<K, V> extends DomainDaoSupport {
	/**
	 * get value by key. if key not exists, this method will return null.
	 * 
	 * @param key
	 * @return
	 * @throws ODBException
	 */
	Future<V> get(K key) throws ODBException;

	Future<V[]> list(List<K> key) throws ODBException;

	Future<BytesHashMap<byte[]>> listBySecondKey(K secondKey) throws ODBException;

	Future<V> put(K key, V value) throws ODBException;

	Future<V> put(K key, K secondaryKey, V value) throws ODBException;

	Future<V[]> batchPuts(List<K> key, List<V> value) throws ODBException;

	Future<V> putIfNotExist(K key, V value) throws ODBException;

	Future<V> delete(K key) throws ODBException;

	Future<V[]> batchDelete(List<K> key) throws ODBException;

	Future<BytesHashMap<byte[]>> deleteBySecondKey(K secondKey, List<K> keys) throws ODBException;

	void sync() throws ODBException;
}
