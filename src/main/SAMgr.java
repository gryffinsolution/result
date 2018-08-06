package main;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import util.Conf;

public class SAMgr {
	private static final Logger LOG = LogManager
			.getLogger(SAMgr.class);

	public static void printSQLException(SQLException e) {
		while (e != null) {
			LOG.error("\n----- SQLException -----");
			LOG.error("  SQL State:  " + e.getSQLState());
			LOG.error("  Error Code: " + e.getErrorCode());
			LOG.error("  Message:    " + e.getMessage());
			e = e.getNextException();
		}
	}
	public static void main(String[] args) {

		DateTime start = new DateTime();
		Conf cf = new Conf();
		if (args.length != 0 && args[0] != null) {
			cf.setConfFile(args[0]);
		} else {
			LOG.error("there is no config.xml as a args[0]");
			System.exit(0);
		}

		
		
		
		
		
		String rdbUrl = cf.getDbURL();
		String rdbUser = cf.getSingleString("user");
		String rdbPasswd = cf.getSingleString("password");
		int thAll = cf.getSinglefValue("no_of_thread");
		int agentPort = cf.getSinglefValue("agent_port");
		String dbType = cf.getSingleString("main_db_type");

		if (cf.getDbURL().startsWith("jdbc:postgresql:")) {
			dbType = "postgresql";
		} else if (cf.getDbURL().startsWith("jdbc:oracle:")) {
			dbType = "oracle";
		} else {
			LOG.fatal("Can't find right JDBC. please check you config.xml");
			System.exit(0);
		}

		ExecutorService pool = Executors.newFixedThreadPool(thAll);
		Set<Future<Boolean>> set = new HashSet<Future<Boolean>>();

		for (int thNo = 1; thNo <= thAll; thNo++) {
			Callable callable = new Worker(thNo, thAll, rdbUrl, rdbUser,
					rdbPasswd, agentPort, dbType);
			Future future = pool.submit(callable);
			set.add(future);
		}
		pool.shutdown();
		DateTime end = new DateTime();
		Duration elapsedTime = new Duration(start, end);
		LOG.fatal(elapsedTime);
	}
}
