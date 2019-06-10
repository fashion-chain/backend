package org.fok.core.dbapi;

import java.util.List;
import java.util.concurrent.Future;

import org.fok.tools.bytes.BytesHashMap;

import lombok.Data;
import onight.tfw.ojpa.api.DomainDaoSupport;
import onight.tfw.ojpa.api.ServiceSpec;

@Data
public class ODBDao implements ODBSupport {

	protected ServiceSpec serviceSpec;
	protected ODBSupport daosupport;
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
		this.daosupport = (ODBSupport) dds;
	}

	@Override
	public Future<byte[]> get(byte[] key) throws ODBException {
		return daosupport.get(key);
	}

	@Override
	public Future<byte[][]> list(List<byte[]> key) throws ODBException {
		return daosupport.list(key);
	}

	@Override
	public Future<BytesHashMap<byte[]>> listBySecondKey(byte[] secondKey) throws ODBException {
		return daosupport.listBySecondKey(secondKey);
	}

	@Override
	public Future<byte[]> put(byte[] key, byte[] value) throws ODBException {
		return daosupport.put(key, value);
	}

	@Override
	public Future<byte[]> put(byte[] key, byte[] secondaryKey, byte[] value) throws ODBException {
		return daosupport.put(key, secondaryKey, value);
	}

	@Override
	public Future<byte[][]> batchPuts(List<byte[]> key, List<byte[]> value) throws ODBException {
		return daosupport.batchPuts(key, value);
	}

	@Override
	public Future<byte[]> putIfNotExist(byte[] key, byte[] value) throws ODBException {
		return daosupport.putIfNotExist(key, value);
	}

	@Override
	public Future<byte[]> delete(byte[] key) throws ODBException {
		return daosupport.delete(key);
	}

	@Override
	public Future<byte[][]> batchDelete(List<byte[]> key) throws ODBException {
		return daosupport.batchDelete(key);
	}

	@Override
	public Future<BytesHashMap<byte[]>> deleteBySecondKey(byte[] secondKey, List<byte[]> keys) throws ODBException {
		return daosupport.deleteBySecondKey(secondKey, keys);
	}

	@Override
	public void sync() throws ODBException {
		daosupport.sync();
	}
}
