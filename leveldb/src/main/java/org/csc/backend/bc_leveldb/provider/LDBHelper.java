package org.csc.backend.bc_leveldb.provider;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import onight.tfw.outils.conf.PropHelper;
import org.apache.commons.lang3.StringUtils;
import org.brewchain.core.dbapi.ODBSupport;
import org.csc.backend.bc_leveldb.api.LDatabase;
import org.csc.backend.bc_leveldb.api.SecondaryDatabase;
import org.csc.backend.bc_leveldb.api.TransactionConfig.CompressionType;
import org.csc.backend.bc_leveldb.api.TransactionConfig.Option;
import org.csc.backend.bc_leveldb.config.Config;
import org.csc.backend.bc_leveldb.jni.LDBNative;
import org.fc.zippo.dispatcher.IActorDispatcher;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@AllArgsConstructor
public class LDBHelper {
	PropHelper params;

	IActorDispatcher dispatcher = null;

	public void copyFile(File in, File out) {
		if (in.isDirectory()) {
			out.mkdirs();
		}
		for (File cpfile : in.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pathname.isDirectory() || pathname.getName().endsWith(".ldb")
						|| pathname.getName().endsWith(".log") || pathname.getName().startsWith("MANIFEST")
						|| StringUtils.containsIgnoreCase("CURRENT,LOCK,LOG,LOG.old,", pathname.getName());
			}
		})) {
			if (cpfile.isFile()) {
				File dstout = new File(out, cpfile.getName());
//				log.info("copy file:from " + cpfile.getAbsolutePath() + ",to ==>"
//						+ dstout.getAbsolutePath());
				dstout.getParentFile().mkdirs();
				try (FileInputStream input = new FileInputStream(cpfile);
						FileOutputStream output = new FileOutputStream(dstout);) {
					byte[] bb = new byte[10240];
					int size = 0;
					while ((size = input.read(bb)) > 0) {
						output.write(bb, 0, size);
					}
				} catch (IOException e) {
					log.error("error in copyingg file:", e);
					System.exit(-1);
				}
			} else {
				copyFile(cpfile, new File(out, cpfile.getName()));
			}
		}

	}

	private String initDatabaseEnvironment(String root, String domainName, int cc) {
		String network = this.params.get("org.brewchain.core.environment.net", null);
		String domainPaths[] = domainName.split("\\.");
		String dbfolder;
		if (domainPaths.length == 3) {
			dbfolder = "db" + File.separator + network + File.separator + root + File.separator + domainPaths[0] + "."
					+ domainPaths[1] + "." + cc;
		} else {
			dbfolder = "db" + File.separator + network + File.separator + root + File.separator + domainName;
		}
		log.info(">> dbfolder:" + dbfolder);
		File dbHomeFile = new File(dbfolder);
		if (!dbHomeFile.exists()) {
			if (!dbHomeFile.mkdirs()) {
				throw new PersistentMapException("make db folder error");
			} else {
				String genesisDbDir = params.get("org.brewchain.core.genesis.dir", "genesis");
				String genesisDbFileStr = "";
				// genesisDbDir + File.separator + network + File.separator + "db"
				// + File.separator + domainPaths[0];
				if (domainPaths.length == 3) {
					genesisDbFileStr = genesisDbDir + File.separator + network + File.separator + "db" + File.separator
							+ domainPaths[0] + "." + domainPaths[1] + "." + cc;
				} else {
					genesisDbFileStr = genesisDbDir + File.separator + network + File.separator + "db" + File.separator
							+ domainName;
				}

				File genesisDbFile = new File(genesisDbFileStr);
				if (genesisDbFile.exists() && genesisDbFile.isDirectory()) {
					try {
						log.info("init genesis db from:" + genesisDbFile.getAbsolutePath() + ",dbhome="
								+ dbHomeFile.getAbsolutePath());
						copyFile(genesisDbFile, dbHomeFile);
					} catch (Exception e) {
						log.error("copy db ex:", e);
					}
				} else {
					log.warn("genesis file not exist:" + genesisDbFileStr);
				}
			}
		}
		return dbHomeFile.getAbsolutePath();
	}

	LDBNative nativeInst;

	private LDatabase[] openDatabase(String dbhomeFile, String dbNameP, boolean allowCreate) {
		Option option = new Option();
		option.setCreate_if_missing(allowCreate);
		// option.setCompression(CompressionType.kSnappyCompression.ordinal());
		option.setCompression(CompressionType.kNoCompression.ordinal());
		option.setParanoid_checks(true);
		// 10M
		option.setWrite_buffer_size(params.get(Config.WRITE_BUFFER_SIZE, 10 * 1024 * 1024));
		// 1w
		option.setMax_open_files(params.get(Config.MAX_OPEN_FILE, 10000));
		// 10M
		option.setBlock_size(params.get(Config.BLOCK_SIZE, 10 * 1024 * 1024));
		// 500M
		option.setMax_file_size(params.get(Config.MAX_FILE_SIZE, 500 * 1024 * 1024));

		// System.out.println("max_file_size=" + option.getMax_file_size());
		log.debug("open new db=>" + dbNameP);
		String dbsname[] = dbNameP.split("\\.");
		// LDatabase db = env.openDatabase(null, dbsname[0], objDbConf);
		if ((dbsname.length == 2 || dbsname.length == 3) && StringUtils.isNotBlank(dbsname[1])) {// dbsname[1]==secondary
			// key
			if (dbsname.length == 3 && StringUtils.isNotBlank(dbsname[1])) {
				dbNameP = dbsname[0] + "." + dbsname[1];
			}
			String dbfilename = new File(dbhomeFile, dbNameP).getAbsolutePath();
			log.debug("create slice db:" + dbfilename);
			long dbinst = nativeInst.openDB(option, dbfilename);
			if (dbinst == 0) {
				log.error("create db error:" + dbfilename);
				System.exit(-1);
			}
			long dbinstsec = nativeInst.openDB(option, dbfilename + ".index");
			return new LDatabase[] { new LDatabase(dbinst, nativeInst, dbfilename),
					new SecondaryDatabase(dbinstsec, nativeInst, dbfilename + ".index") };
		} else {
			String dbfilename = new File(dbhomeFile, dbsname[0]).getAbsolutePath();
			log.debug("create normal db:" + dbfilename);
			long dbinst = nativeInst.openDB(option, dbfilename);
			if (dbinst == 0) {
				log.error("create db error:" + dbfilename);
				System.exit(-1);
			}
			return new LDatabase[] { new LDatabase(dbinst, nativeInst, dbfilename) };
		}
	}

	public OLevelDBImpl createODBImpl(String dir, String domainName, int cc) {
		String dbhomeFile = initDatabaseEnvironment(dir, domainName, cc);
		LDatabase[] dbs = openDatabase(dbhomeFile, "bc_" + domainName, true);
		if (dbs.length == 1) {
			if (params.get("org.brewchain.backend.deferdb", "account,block,tx,").contains(domainName.split("\\.")[0])) {
				long delay = params.get("org.brewchain.backend.deferdb.delayms", 200);
				DeferOBDBImpl ret = new DeferOBDBImpl(params.get("org.brewchain.backend.deferdb.size", 100),
						params.get("org.csc.backend.l2cache.size", 1000), delay, domainName, dbs[0]);
				dispatcher.scheduleWithFixedDelay(ret, delay, delay, TimeUnit.MILLISECONDS);
				return ret;
			} else {
				return new OLevelDBImpl(domainName, dbs[0]);
			}
		} else {
			if (params.get("org.brewchain.backend.deferdb", "account,block,tx,").contains(domainName.split("\\.")[0])) {
				long delay = params.get("org.brewchain.backend.deferdb.delayms", 200);
				DeferOBDBImpl ret = new DeferOBDBImpl(params.get("org.brewchain.backend.deferdb.size", 100),
						params.get("org.csc.backend.l2cache.size", 1000), delay, domainName, dbs[0], dbs[1]);
				dispatcher.scheduleWithFixedDelay(ret, delay, delay, TimeUnit.MILLISECONDS);
				return ret;
			} else {
				return new OLevelDBImpl(domainName, dbs[0], dbs[1]);
			}
		}
	}

	public ODBSupport createDBI(HashMap<String, ODBSupport> dbsByDomains, String dir, String domainName) {
		ODBSupport dbi = null;
		synchronized (dbsByDomains) {
			dbi = dbsByDomains.get(domainName);
			if (dbi == null) {
				String dbss[] = domainName.split("\\.");
				int cc = 1;

				if (dbss.length == 3) {// with slicer
					try {
						cc = Integer.parseInt(dbss[2]);
						log.info("create slice db:==>" + cc + "," + domainName);
					} catch (NumberFormatException e) {
						e.printStackTrace();
					}
				}
				OLevelDBImpl dbis[] = new OLevelDBImpl[cc];
				for (int i = 0; i < cc; i++) {
					dbis[i] = createODBImpl(dir, domainName, i);
				}
				if (cc > 1) {
					dbi = new SlicerDBImpl(domainName, dbis, dispatcher);
				} else {
					dbi = dbis[0];
				}
				dbsByDomains.put(domainName, dbi);
			}
		}
		return dbi;
	}

}
