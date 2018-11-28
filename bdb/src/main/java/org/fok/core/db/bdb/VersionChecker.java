package org.fok.core.db.bdb;

import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;

import com.google.protobuf.ByteString;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VersionChecker {

	public static final String BIG_VERSION = "v1.";
	public static final String SUB_VERSION = "0.";
	public static final String MIN_VERSION = "0";

	public static final String FULL_VERSION = BIG_VERSION + SUB_VERSION + MIN_VERSION;

	public static boolean check(OBDBImpl db) {
		try {
			byte[] key = ByteString.copyFrom("BC_VERSION", "UTF-8").toByteArray();
			Future<byte[]> ver = db.get(key);
			if (ver == null || ver.get() == null) {
				db.put(key, ByteString.copyFrom(FULL_VERSION, "UTF-8").toByteArray());
				log.info("DBVersion Check SUCCESS:");
			} else if (!StringUtils.startsWith(ByteString.copyFrom(ver.get()).toStringUtf8(), BIG_VERSION)) {
				//
				log.error("DBVersion Check ERROR!Current=" + FULL_VERSION + ",db version="
						+ ByteString.copyFrom(ver.get()).toStringUtf8() + ". It will Cause Unknowm Problem!!");
				System.exit(-1);
			} else if (!StringUtils.startsWith(ByteString.copyFrom(ver.get()).toStringUtf8(),
					BIG_VERSION + SUB_VERSION)) {
				//
				log.warn("DBVersion Check Warning!Current=" + FULL_VERSION + ",db version="
						+ ByteString.copyFrom(ver.get()).toStringUtf8() + ". It will Cause Unknowm Problem!!");
			} else {
				log.info("DBVersion Check SUCCESS:");
			}
		} catch (Exception e) {
			log.error("DBVersion Check Failed", e);
			System.exit(-1);
		}
		return true;
	}
}
