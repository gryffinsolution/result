package util;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ADao {
	private static final Logger LOG = LogManager.getLogger(ADao.class);
	Connection conn = null;
	String protocol = null;

	public boolean isWorking() {
		return true;
	}

	public static String printSQLException(SQLException e) {
		LOG.error("\n----- SQLException -----");
		LOG.error("  SQL State:  " + e.getSQLState());
		LOG.error("  Error Code: " + e.getErrorCode());
		LOG.error("  Message:    " + e.getMessage());
		if (e.getMessage().contains("Table/View")
				|| e.getMessage().contains(" does not exist.")) {
			LOG.fatal(e.getMessage());
			return null;
		}
		if (e.getMessage().contains("Error connecting to server")) {
			LOG.info(e.getMessage());
			return null;
		}
		return null;
	}

	public String getResultTO(int port, String host)
			throws UnknownHostException {

		Properties props = new Properties();
		props.put("user", "agent");
		props.put("password", "catallena7");

		StringBuffer sb = new StringBuffer();
		if (host.matches("localhost.localdomain")) {// TEST
			host = "192.168.178.131";
		}
		protocol = "jdbc:derby://" + host + ":" + port + "/";

		PreparedStatement pst = null;
		ResultSet rs = null;
		Statement s = null;
		ArrayList<String> mjobLists = new ArrayList<String>();
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try {
			Future<Connection> future = executor
					.submit(new Callable<Connection>() {
						private Connection conn;

						@Override
						public Connection call() throws Exception {
							LOG.info("protocol=" + protocol);
							conn = DriverManager.getConnection(protocol
									+ "derbyDB;create=true", props);
							return conn;
						}
					});

			conn = future.get(3, TimeUnit.SECONDS);
			// conn.setNetworkTimeout(executor,5000);
			DriverManager.setLoginTimeout(1);

			String sql = "SELECT MJOBID,RUN_TS,END_TS,"
					+ " STATUS,CMD,CMD_MTIME_TS,"
					+ " TIMEOUT_SEC,IN_MGR,OUT_MGR, "
					+ " LAST_MGR_LTS,SET_RERUN_CNT,RERUN_CNT,IS_SENT,ELAPSED_TIME_SEC,STDOUT"
					+ " FROM AUTO_JOBS" + " WHERE IS_SENT IS NULL"
					+ " AND ( STATUS ='DONE' " + " OR STATUS ='ERR_EXEC' "
					+ "		OR STATUS ='ERR_TIME' " + "	OR STATUS ='LC_ERROR')";
			s = conn.createStatement();
			s.setQueryTimeout(1);

			rs = s.executeQuery(sql);
			while (rs.next()) {
				String MJOBID = rs.getString("MJOBID");
				mjobLists.add(MJOBID);
				sb.append(MJOBID);
				sb.append(",:,");
				sb.append(rs.getTimestamp("RUN_TS"));
				sb.append(",:,");
				sb.append(rs.getTimestamp("END_TS"));
				sb.append(",:,");
				sb.append(rs.getString("STATUS"));
				sb.append(",:,");
				sb.append(rs.getInt("ELAPSED_TIME_SEC"));
				sb.append("-FLOG-SA-OUT-");
				sb.append(rs.getString("STDOUT"));
				sb.append("-FLOG-SA-MGR-");
			}
			if (sb.length() > 0)
				LOG.info(host + ":out=" + sb);
			if (sb.length() > 3) {
				for (String jobid : mjobLists) {
					sql = "UPDATE AUTO_JOBS SET IS_SENT=1 WHERE MJOBID='"
							+ jobid + "'";
					LOG.trace(host + ":" + sql);
					pst = conn.prepareStatement(sql);
					pst.executeUpdate();
				}
				conn.commit();
			}
		} catch (SQLException sqle) {
			printSQLException(sqle);
			return null;
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			try {
				if (conn != null) {
					conn.close();
					conn = null;
					return sb.toString();
				}
			} catch (SQLException sqle) {
				printSQLException(sqle);
				return null;
			}
			executor.shutdownNow();
			return null;
		} finally {
			executor.shutdownNow();
			try {
				if (pst != null) {
					pst.close();
					pst = null;
				}
				if (conn != null) {
					conn.close();
					conn = null;
					return sb.toString();
				}
			} catch (SQLException e) {
				printSQLException(e);
				return null;
			}
		}
		executor.shutdownNow();
		return sb.toString();
	}
}
