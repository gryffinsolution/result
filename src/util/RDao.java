package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RDao {
	private static final Logger LOG = LogManager.getLogger(RDao.class);

	public static void printSQLException(SQLException e) {
		while (e != null) {
			LOG.error("\n----- SQLException -----");
			LOG.error("  SQL State:  " + e.getSQLState());
			LOG.error("  Error Code: " + e.getErrorCode());
			LOG.error("  Message:    " + e.getMessage());
			e = e.getNextException();
		}
	}

	public Connection getConnection(String dbURL, String user, String password) {
		LOG.info("DB_URL=" + dbURL);
		Connection con = null;
		try {
			if (dbURL.startsWith("jdbc:postgresql:")) {
				Class.forName("org.postgresql.Driver");
			} else if (dbURL.startsWith("jdbc:oracle:")) {
				Class.forName("oracle.jdbc.driver.OracleDriver");
			}
		} catch (ClassNotFoundException e) {
			LOG.error("DB Driver loading error!");
			e.printStackTrace();
		}
		try {
			con = DriverManager.getConnection(dbURL, user, password);
		} catch (SQLException e) {
			LOG.error("getConn Exception)");
			e.printStackTrace();
		}
		return con;
	}

	public void disconnect(Connection conn) {
		try {
			conn.close();
		} catch (SQLException e) {
			LOG.error("disConn Exception)");
			printSQLException(e);
		}
	}

	public ArrayList<String> getHostsTest(Connection conR) {
		ArrayList<String> host = new ArrayList<String>();
		host.add("localhost.localdomain");
		return host;
	}

	public boolean insertStdout(Connection conR, String host, String allLines,
			String dbType) {
		PreparedStatement pst = null;
		if (allLines.length() <= 0) {
			return false;
		}
		try {
			conR.setAutoCommit(false);
			String[] lines = allLines.split("-FLOG-SA-MGR-");
			for (String line : lines) {
				LOG.info(line);
				String[] lineSTDOUT = line.toString().split("-FLOG-SA-OUT-");
				String[] items = lineSTDOUT[0].toString().split(",:,");

				String[] runDates = items[1].split("\\.");
				LOG.info("run_date=" + runDates[0]);
				String[] endDates = items[2].split("\\.");
				LOG.info("end_date=" + endDates[0]);

				String sql = null;
				if (lineSTDOUT.length == 2) {

					lineSTDOUT[1] = lineSTDOUT[1].replaceAll("\'", "\''");
					if (lineSTDOUT[1].length() > 4000) {
						lineSTDOUT[1] = lineSTDOUT[1].substring(0, 4000);
					}

					sql = "MERGE INTO SA_RESULT SR USING DUAL D ON (SR.MJOB_ID='"
							+ items[0]
							+ "' AND SR.HOST='"
							+ host
							+ "') "
							+ "WHEN MATCHED THEN "
							+ "UPDATE SET SR.RUN_TS=TO_DATE('"
							+ runDates[0]
							+ "', 'YYYY-MM-DD HH24:MI:SS'), "
							+ "SR.END_TS=TO_DATE('"
							+ endDates[0]
							+ "', 'YYYY-MM-DD HH24:MI:SS'), "
							+ "STATUS='"
							+ items[3]
							+ "',"
							+ "ELAPSED_TIME_SEC ="
							+ items[4]
							+ ","
							+ "STDOUT='"
							+ lineSTDOUT[1]
							+ "',"
							+ "ARRIVAL_TIME=CURRENT_TIMESTAMP "
							+ "WHEN NOT MATCHED THEN "
							+ "INSERT "
							+ " (MJOB_ID,HOST,RUN_TS,END_TS,STATUS,ELAPSED_TIME_SEC,STDOUT,ARRIVAL_TIME) "
							+ "VALUES ('"
							+ items[0]
							+ "','"
							+ host
							+ "',"
							+ "TO_DATE('"
							+ runDates[0]
							+ "', 'YYYY-MM-DD HH24:MI:SS'), "
							+ "TO_DATE('"
							+ endDates[0]
							+ "', 'YYYY-MM-DD HH24:MI:SS'), "
							+ "'"
							+ items[3]
							+ "',"
							+ items[4]
							+ ","
							+ "'"
							+ lineSTDOUT[1] + "',CURRENT_TIMESTAMP)";
				} else {
					sql = "MERGE INTO SA_RESULT SR USING DUAL D ON (SR.MJOB_ID='"
							+ items[0]
							+ "' AND SR.HOST='"
							+ host
							+ "')"
							+ "WHEN MATCHED THEN "
							+ "UPDATE SET SR.RUN_TS=TO_DATE('"
							+ runDates[0]
							+ "', 'YYYY-MM-DD HH24:MI:SS'),"
							+ "SR.END_TS=TO_DATE('"
							+ endDates[0]
							+ "', 'YYYY-MM-DD HH24:MI:SS'),"
							+ "STATUS='"
							+ items[3]
							+ "',"
							+ "ELAPSED_TIME_SEC ="
							+ items[4]
							+ ","
							+ "STDOUT='',"
							+ "ARRIVAL_TIME=CURRENT_TIMESTAMP "
							+ "WHEN NOT MATCHED THEN "
							+ "INSERT "
							+ "(MJOB_ID,HOST,RUN_TS,END_TS,STATUS,ELAPSED_TIME_SEC,STDOUT,ARRIVAL_TIME) "
							+ "VALUES ('"
							+ items[0]
							+ "','"
							+ host
							+ "',"
							+ "TO_DATE('"
							+ runDates[0]
							+ "', 'YYYY-MM-DD HH24:MI:SS'), "
							+ "TO_DATE('"
							+ endDates[0]
							+ "', 'YYYY-MM-DD HH24:MI:SS'), "
							+ "'"
							+ items[3]
							+ "',"
							+ items[4]
							+ ","
							+ "'',CURRENT_TIMESTAMP)";
				}
				LOG.trace(sql);
				pst = conR.prepareStatement(sql);
				pst.executeUpdate();
				conR.commit();
				pst.close();
			}
		} catch (SQLException sqle) {
			printSQLException(sqle);
		} catch (ArrayIndexOutOfBoundsException e) {
			LOG.error("IndexOut " + host + " " + allLines);
		} finally {
			try {
				if (pst != null)
					pst.close();
			} catch (SQLException e) {
				printSQLException(e);
			}
			pst = null;
		}
		return true;
	}

	public ArrayList<String> getSentHostMT(Connection con, int thNo, int thAll) {
		Statement stmt;
		ArrayList<String> hostList = new ArrayList<String>();
		try {
			String sql = "SELECT DISTINCT HOST FROM SA_RESULT SR,HOST_INFOS HI"
					+ " WHERE SR.HOST=HI.HOSTNAME AND SR.STATUS ='SENT' "
					+ " AND SUBMIT_TS > SYSDATE -1/24 AND HI.STATUS ='up' "
					+ " AND ( IS_V3 IS NULL OR IS_V3 = 0) ";
			int i = 0;
			LOG.info(sql);
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String host = rs.getString("HOST");
				LOG.info(thNo + 1 + "/" + thAll + " " + host);
				if (i % thAll == thNo) {
					hostList.add(host);
					LOG.info("ADD:" + thNo + 1 + "/" + thAll + " " + host);
				}
				i++;
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			printSQLException(e);
		}
		return hostList;
	}

	public void setWorkingTimestamp(Connection conR, String rdbUrl, int thNo) {
		PreparedStatement pst = null;
		try {
			conR.setAutoCommit(false);

			String sqlLastUpdateTime = null;

			sqlLastUpdateTime = "UPDATE MANAGER_SERVICE_HEALTH_CHECK SET LAST_UPDATED_TIME=SYSDATE WHERE SERVICE_NAME='samgr"
					+ thNo + "'";// oracle

			LOG.trace(sqlLastUpdateTime);
			pst = conR.prepareStatement(sqlLastUpdateTime);
			pst.executeUpdate();
			conR.commit();
			pst.close();
		} catch (SQLException sqle) {
			printSQLException(sqle);
		} catch (ArrayIndexOutOfBoundsException e) {
			LOG.error(e);
		} finally {
			try {
				if (pst != null)
					pst.close();
			} catch (SQLException e) {
				printSQLException(e);
			}
			pst = null;
		}
	}
}