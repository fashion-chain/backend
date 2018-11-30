package org.fok.core.db.bdb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.fok.core.dbapi.ODBSupport;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import onight.tfw.outils.conf.PropHelper;

@Slf4j
@AllArgsConstructor
public class DBHelper {
	PropHelper params;

	ScheduledExecutorService exec;

	private Environment initDatabaseEnvironment(String root, String domainName, int cc) {
		String network = "";
		try {
			File networkFile = new File(".chainnet");
			if (!networkFile.exists() || !networkFile.canRead()) {
				// read default config
				network = this.params.get("org.fok.core.environment.net", "testnet");
			}
			if (network == null || network.isEmpty()) {
				while (!networkFile.exists() || !networkFile.canRead()) {
					log.debug("waiting chain_net config...");
					Thread.sleep(1000);
				}

				FileReader fr = new FileReader(networkFile.getPath());
				BufferedReader br = new BufferedReader(fr);
				network = br.readLine().trim().replace("\r", "").replace("\t", "");
				br.close();
				fr.close();
			}
			// log.debug("choose the chain_net::" + network);
		} catch (Exception e) {
			log.error("error on read chain_net::" + e.getMessage());
		}
		String domainPaths[] = domainName.split("\\.");
		String dbfolder;
		if (domainPaths.length == 3) {
			dbfolder = "db" + File.separator + network + File.separator + root + File.separator + domainPaths[0] + "."
					+ domainPaths[1] + "." + cc;
		} else {
			dbfolder = "db" + File.separator + network + File.separator + root + File.separator + domainName;
		}
		log.info(">> dbfolder" + dbfolder);
		File dbHomeFile = new File(dbfolder);
		if (!dbHomeFile.exists()) {
			if (!dbHomeFile.mkdirs()) {
				throw new PersistentMapException("make db folder error");
			} else {
				String genesisDbDir = params.get("org.bc.obdb.dir", "genesis");
				String genesisDbFileStr = genesisDbDir + File.separator + network + File.separator + "db"
						+ File.separator + domainName + File.separator + "00000000.jdb";
				File genesisDbFile = new File(genesisDbFileStr);
				if (!genesisDbFile.exists() && domainPaths.length == 3) {
					genesisDbFileStr = genesisDbDir + File.separator + network + File.separator + "db" + File.separator
							+ domainPaths[0] + File.separator + "00000000.jdb";
					genesisDbFile = new File(genesisDbFileStr);
					if (!genesisDbFile.exists()) {
						genesisDbFileStr = genesisDbDir + File.separator + network + File.separator + "db"
								+ File.separator + domainPaths[0] + "." + domainPaths[1] + File.separator
								+ "00000000.jdb";
						genesisDbFile = new File(genesisDbFileStr);
					}
					if (!genesisDbFile.exists()) {
						genesisDbFileStr = genesisDbDir + File.separator + network + File.separator + "db"
								+ File.separator + domainPaths[0] + "." + domainPaths[1] + "." + cc + File.separator
								+ "00000000.jdb";
						genesisDbFile = new File(genesisDbFileStr);
					}
					if (!genesisDbFile.exists()) {
						genesisDbFileStr = genesisDbDir + File.separator + network + File.separator + "db"
								+ File.separator + domainPaths[0] + "." + domainPaths[1] + "." + domainPaths[2]
								+ File.separator + "00000000.jdb";
						genesisDbFile = new File(genesisDbFileStr);
					}
				}
				if (genesisDbFile.exists() && genesisDbFile.isFile()) {
					try {
						log.info("init genesis db from:" + genesisDbFile.getAbsolutePath() + ",size="
								+ genesisDbFile.length());

						try (FileInputStream input = new FileInputStream(genesisDbFile);
								FileOutputStream output = new FileOutputStream(
										dbfolder + File.separator + "00000000.jdb");) {
							byte[] bb = new byte[10240];
							int size = 0;
							while ((size = input.read(bb)) > 0) {
								output.write(bb, 0, size);
							}
						} catch (IOException e) {
							e.printStackTrace();
						}

					} catch (Exception e) {
						log.error("copy db ex:", e);
					}
				}
			}
		}

		EnvironmentConfig envConfig = new EnvironmentConfig();
		// TODO db性能调优
		envConfig.setDurability(Durability.COMMIT_SYNC);
		envConfig.setTxnTimeout(params.get("org.brewchain.backend.bdb.txn.timeoutms", 30 * 1000),
				TimeUnit.MILLISECONDS);
		envConfig.setLockTimeout(params.get("org.brewchain.backend.bdb.lock.timeoutms", 30 * 1000),
				TimeUnit.MILLISECONDS);
		envConfig.setAllowCreate(true);
		envConfig.setTransactional(true);

		envConfig.setCacheSize(params.get("org.brewchain.backend.bdb.cache.max", 64 * 1024 * 1024));
		envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER,
				params.get("org.brewchain.backend.bdb.je.env.runCleaner", "false"));
		envConfig.setConfigParam(EnvironmentConfig.CLEANER_THREADS,
				params.get("org.brewchain.backend.bdb.je.cleaner.threads", "1"));
		// EnvironmentConfig.LOG_ITERATOR_READ_SIZE
		envConfig.setConfigParam(EnvironmentConfig.CLEANER_LOOK_AHEAD_CACHE_SIZE, "8192000");
		envConfig.setConfigParam(EnvironmentConfig.EVICTOR_CORE_THREADS,
				params.get("org.brewchain.backend.bdb.je.evitor.core.threads", "1"));
		envConfig.setConfigParam(EnvironmentConfig.LOCK_N_LOCK_TABLES, "10");
		envConfig.setConfigParam(EnvironmentConfig.LOG_WRITE_QUEUE_SIZE,
				params.get("org.brewchain.backend.bdb.je.log.write.queue.size", "812900"));
		envConfig.setConfigParam(EnvironmentConfig.NODE_MAX_ENTRIES,
				params.get("org.brewchain.backend.bdb.je.node.max.entries", "8129"));

//		envConfig.setConfigParam(EnvironmentConfig.FILE_LOGGING_LEVEL,
//				params.get("org.brewchain.backend.bdb.je.file.logging.level", "error"));

		log.info(">> dbHomeFile" + dbHomeFile);

		return new Environment(dbHomeFile, envConfig);
	}

	private Database[] openDatabase(Environment env, String dbNameP, boolean allowCreate, boolean allowDuplicates) {
		DatabaseConfig objDbConf = new DatabaseConfig();
		objDbConf.setAllowCreate(allowCreate);
		objDbConf.setSortedDuplicates(allowDuplicates);
		// objDbConf.setDeferredWrite(true);
		objDbConf.setTransactional(true);

		String dbsname[] = dbNameP.split("\\.");
		Database db = env.openDatabase(null, dbsname[0], objDbConf);

		if (dbsname.length == 2 || dbsname.length == 3 && StringUtils.isNotBlank(dbsname[1])) {
			SecondaryConfig sd = new SecondaryConfig();
			sd.setAllowCreate(allowCreate);
			sd.setAllowPopulate(true);
			sd.setSortedDuplicates(true);
			sd.setDeferredWrite(false);
			sd.setTransactional(true);
			ODBTupleBinding tb = new ODBTupleBinding();
			SecondaryKeyCreator keyCreator = new ODBSecondKeyCreator(tb);
			sd.setKeyCreator(keyCreator);
			if (dbsname.length == 3 && StringUtils.isNotBlank(dbsname[1])) {
				dbNameP = dbsname[0] + "." + dbsname[1];
			}
			SecondaryDatabase sdb = env.openSecondaryDatabase(null, dbNameP, db, sd);
			return new Database[] { db, sdb };
		} else {
			return new Database[] { db };
		}
	}

	public OBDBImpl createODBImpl(String dir, String domainName, int cc) {
		Environment env = initDatabaseEnvironment(dir, domainName, cc);
		Database[] dbs = openDatabase(env, "bc_bdb_" + domainName, true, false);
		if (dbs.length == 1) {
			if (params.get("org.brewchain.backend.deferdb", "account,block,tx,").contains(domainName.split("\\.")[0])) {
				long delay = params.get("org.brewchain.backend.deferdb.delayms", 200);
				DeferOBDBImpl ret = new DeferOBDBImpl(params.get("org.brewchain.backend.deferdb.size", 100), delay,
						domainName, dbs[0]);
				exec.scheduleWithFixedDelay(ret, delay, delay, TimeUnit.MILLISECONDS);
				return ret;
			} else {
				return new OBDBImpl(domainName, dbs[0]);
			}
		} else {
			if (params.get("org.brewchain.backend.deferdb", "account,block,tx,").contains(domainName.split("\\.")[0])) {
				long delay = params.get("org.brewchain.backend.deferdb.delayms", 200);

				DeferOBDBImpl ret = new DeferOBDBImpl(params.get("org.brewchain.backend.deferdb.size", 100), delay,
						domainName, dbs[0], dbs[1]);
				exec.scheduleWithFixedDelay(ret, delay, delay, TimeUnit.MILLISECONDS);
				return ret;
			} else {
				return new OBDBImpl(domainName, dbs[0], dbs[1]);
			}
		}
	}

	public ODBSupport createDBI(HashMap<String, ODBSupport> dbsByDomains, String dir, String domainName) {
		ODBSupport dbi = null;
		synchronized (dbsByDomains) {
			dbi = dbsByDomains.get(domainName);
			if (dbi == null) {
				// if (this.dbEnv == null) {
				// dbi = new OBDBImpl(dds.getDomainName(), null);
				// } else {
				String dbss[] = domainName.split("\\.");
				int cc = 1;
				if (dbss.length == 3) {
					try {
						cc = Integer.parseInt(dbss[2]);
						log.info("create slice db:==>" + cc + "," + domainName);
					} catch (NumberFormatException e) {
						e.printStackTrace();
					}
				}
				OBDBImpl dbis[] = new OBDBImpl[cc];
				for (int i = 0; i < cc; i++) {
					dbis[i] = createODBImpl(dir, domainName, i);
				}
				if (cc > 1) {
					dbi = (ODBSupport) new SlicerOBDBImpl(domainName, dbis, exec);
				} else {
					dbi = (ODBSupport) dbis[0];
				}
				dbsByDomains.put(domainName, dbi);
				// log.debug("inject dao::" + domainName);
			}
		}
		return dbi;
	}

}
