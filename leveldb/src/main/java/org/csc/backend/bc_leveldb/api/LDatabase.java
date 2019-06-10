package org.csc.backend.bc_leveldb.api;

import org.apache.commons.codec.binary.Hex;
import org.csc.backend.bc_leveldb.jni.LDBNative;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Slf4j
public class LDatabase {
	long dbinst;

	LDBNative nativeInst;

	String filepath;
	public void close() {
		if (dbinst > 0) {
			nativeInst.closeDB(dbinst);
		}
	}

	public void sync() {

	}

	public int syncPut(byte[] key, byte[] value) {
		return nativeInst.syncPut(dbinst, key, value);
	}

	// Remove the database entry (if any) for "key". Returns OK on
	// success, and a non-OK status on error. It is not an error if "key"
	// did not exist in the database.
	// Note: consider setting options.sync = true.
	public int syncDelete(byte[] key) {
		return nativeInst.syncDelete(dbinst, key);
	}

	// flatten
	// Apply the specified updates to the database.
	// Returns OK on success, non-OK on failure.
	// Note: consider setting options.sync = true.
	public int syncBatchPut(byte[][] keys, byte[][] values) {
		return nativeInst.syncBatchPut(dbinst, keys, values);

	}

	// If the database contains an entry for "key" store the
	// corresponding value in *value and return OK.
	//
	// If there is no entry for "key" leave *value unchanged and return
	// a status for which StatusCode::IsNotFound() returns true.

	// May return some other StatusCode on an error.
	// verify_checksums=false,fill_cache=false,snapshot=NULL
	public byte[] fastGet(byte[] key) {
		return nativeInst.fastGet(dbinst, key);

	}

	// verify_checksums=false,fill_cache=true,snapshot=NULL
	public byte[] fillGet(byte[] key) {
		return nativeInst.fillGet(dbinst, key);
	}

}
