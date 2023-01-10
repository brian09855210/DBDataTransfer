package com.hyena.backstage.datatransfer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hyena.backstage.common.TextHelper;

/**
 * DB資料轉移
 *
 * @author brian.chang
 * @version 2022/04/28 
 *
 */
@Component
public class DBDataTransfer {
	private static final String MYSQL_DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
	private Connection sourceConn = null;
	private Connection backupConn = null;
	public static boolean isTransfer = false;
	
	// 來源庫配置
	private static String url;
	private static String userName;
	private static String password;
	@Value("${spring.datasource.url}")
	public void setUrl(final String url) {
		DBDataTransfer.url = url;
	}
    @Value("${spring.datasource.username}")
    public void setUserName(final String userName) {
		DBDataTransfer.userName = userName;
	}
    @Value("${spring.datasource.password}")
    public void setPassword(final String password) {
		DBDataTransfer.password = password;
	}

    // 目標庫配置
	private static String backupUrl;
	private static String backupUserName;
	private static String backupPassword;
    @Value("${spring.backup.datasource.url}")
    public void setTargetUrl(final String backupUrl) {
  		DBDataTransfer.backupUrl = backupUrl;
  	}
    @Value("${spring.backup.datasource.username}")
    public void setTargetUserName(final String backupUserName) {
  		DBDataTransfer.backupUserName = backupUserName;
  	}
    @Value("${spring.backup.datasource.password}")
    public void setTargetPassword(final String backupPassword) {
  		DBDataTransfer.backupPassword = backupPassword;
  	}
  
	// 搬遷Table設定 (只搬遷名字開頭為log表的資料) (保留1個月)
	public static String[] transTables = {};
	@Value("${data.transfer.backup.table}")
    public void setTransTables(final String[] transTables) {
  		DBDataTransfer.transTables = transTables;
  	}
	
	/**
	 * 來源庫DB連線設定
	 * 
	 * @return
	 */
	public Connection getSourceConn(){
		try {
			Class.forName(MYSQL_DRIVER_CLASS_NAME);
			sourceConn = DriverManager.getConnection(url, userName, password);
			return sourceConn;
		} catch (ClassNotFoundException | SQLException e) {
			LogManager.getLogger().error(e.getMessage());
		}
		return null;
	}
  
	/**
	 * 目標庫DB連線設定
	 * 
	 * @return
	 */
	public Connection getTargetConn(){
		try {
			Class.forName(MYSQL_DRIVER_CLASS_NAME);
		    backupConn = DriverManager.getConnection(backupUrl, backupUserName, backupPassword);
		    return backupConn;
		} catch (ClassNotFoundException | SQLException e) {
			LogManager.getLogger().error(e.getMessage());
        }
		return null;
	}
  
	/**
	 * 獲得Table 1個月前的資料總量
	 * 
	 * @param transTableName
	 * @return
	*/
	public int getTableDataCount(String transTableName) {
		Connection sourceConn = this.getSourceConn();
		PreparedStatement sourceStmt = null;
		ResultSet sourceRs = null;
		int count = 0;
		
		try {
			sourceStmt = sourceConn.prepareStatement("SELECT COUNT(*) FROM " + transTableName + " WHERE created < DATE_SUB(now(), INTERVAL 1 MONTH)");
			sourceRs = sourceStmt.executeQuery();
			while (sourceRs.next()) {
				count = sourceRs.getInt(1);
				if (count != 0) {
					LogManager.getLogger().info("【" + transTableName + "】共要轉移【" + count + "】筆資料");
				}
			}
		} catch (SQLException e) {
			LogManager.getLogger().error(e.getMessage());
		} finally {
			// close資源
			close(sourceConn, sourceStmt, sourceRs);
		}
		return count;
	}
 
	/**
	 * 1個月前資料搬遷
	 * 
	 * @param transTableName
	 * @param tableDataCount
	 * @param offset
	 * @param curLine
	 * @throws SQLException
	 */
	public String transfer(String transTableName, int tableDataCount, int offset, int curLine) throws SQLException {
		// 來源庫資料
		Connection sourceConn = this.getSourceConn();
		PreparedStatement sourceStmt = null;
		ResultSet sourceRs = null;
		
		Connection backupConn = getTargetConn();
		PreparedStatement backupPstmt = null;
		long start = System.currentTimeMillis(); // 取當下時間 (計算耗時時間)
		int count = 0; // 完成筆數
		String maxCreatedTime = ""; // 此批資料最大時間
		
		try {
			backupConn.setAutoCommit(false); 
						
			// Table 1個月前資料查詢
			String sql = "SELECT * FROM " + transTableName + " \n"
						+ "WHERE created < DATE_SUB(now(), INTERVAL 1 MONTH) \n"
						+ "ORDER BY created\n"
						+ "LIMIT 0, " + offset + ";";
			sourceStmt = sourceConn.prepareStatement(sql);
			sourceRs = sourceStmt.executeQuery();
			// 結果集獲取到的長度
			int size = sourceRs.getMetaData().getColumnCount();
			
			// INSERT INTO SQL組成
			StringBuffer sbf = new StringBuffer();
			sbf.append("INSERT IGNORE INTO " + transTableName + " VALUES (");
			String comma = "";
			for (int i = 0; i < size; i++) {
				sbf.append(comma).append("?");
				comma = ",";
			}
			sbf.append(")");
			
			// 更新DB參數設定 (SQL_SAFE_UPDATES、FOREIGN_KEY_CHECK、AUTOCOMMIT) = 0
			backupPstmt = backupConn.prepareStatement(updateDBSettingsStr(0));
			backupPstmt.executeUpdate();
			backupPstmt.close();
			
			backupPstmt = backupConn.prepareStatement(sbf.toString());
			// 迴圈來源Table讀取資料
			while (sourceRs.next()) {
				++count;
				for (int i = 1; i <= size; i++) {
					backupPstmt.setObject(i, sourceRs.getObject(i)); // 塞值
	            }
	            // 儲存SQL
				backupPstmt.addBatch();
				
				// 每1000筆execute一次
				if (count % 1000 == 0) {
					backupPstmt.executeBatch(); 
					backupPstmt.clearBatch();
				}
				
				if (sourceRs.isLast()) {
					maxCreatedTime = sourceRs.getString("created");
				}
			}
			backupPstmt.executeBatch(); 
			backupPstmt.clearBatch();
			backupPstmt.close();
			
			// 更新DB參數設定 (SQL_SAFE_UPDATES、FOREIGN_KEY_CHECK、AUTOCOMMIT) = 1
			backupPstmt = backupConn.prepareStatement(updateDBSettingsStr(1));
			backupPstmt.executeUpdate();
			
			// 提交
			backupConn.commit(); 
			backupConn.setAutoCommit(true); 
			
			LogManager.getLogger().info("目標庫【" + transTableName + "】新增第" + curLine + "批資料成功，共" + count + "筆資料，耗時: " + (System.currentTimeMillis() - start) / 1000.0 + "s");
			isTransfer = true;
		} catch (Exception e) {
			isTransfer = false;
			LogManager.getLogger().error("目標庫【" + transTableName + "】新增第" + curLine + "批資料失敗: " + e.getMessage());
			if (backupConn != null) {
				backupConn.rollback();
				backupConn.setAutoCommit(true); 
			}
		} finally {
			// close資源
	        close(backupConn, backupPstmt, null);
	        close(sourceConn, sourceStmt, sourceRs);
		}
		return maxCreatedTime;
    }
	
	/**
	 * 刪除來源庫資料
	 * 
	 * @param transTableName
	 * @param offset
	 * @param curLine
	 * @throws SQLException
	 */
	public void deleteSourceTableData(String transTableName, int offset, int curLine) throws SQLException {
		Connection sourceConn = this.getSourceConn();
		PreparedStatement sourceStmt = null;
		long start = System.currentTimeMillis(); // 取當下時間 (計算耗時時間)
		
		try {
			sourceConn.setAutoCommit(false);
			String sql = "DELETE FROM " + transTableName + " \n"
						+ "WHERE created < DATE_SUB(now(), INTERVAL 1 MONTH) \n"
						+ "ORDER BY created\n"
						+ "LIMIT " + offset + ";";
			
			// 更新DB參數設定 (SQL_SAFE_UPDATES、FOREIGN_KEY_CHECK、AUTOCOMMIT) = 0
			sourceStmt = sourceConn.prepareStatement(updateDBSettingsStr(0));
			sourceStmt.executeUpdate();
			sourceStmt.close();
			
			// 執行刪除SQL
			sourceStmt = sourceConn.prepareStatement(sql);
			int count = sourceStmt.executeUpdate();
			sourceStmt.close();
			
			// 更新DB參數設定 (SQL_SAFE_UPDATES、FOREIGN_KEY_CHECK、AUTOCOMMIT) = 1
			sourceStmt = sourceConn.prepareStatement(updateDBSettingsStr(1));
			sourceStmt.executeUpdate();
			
			// 提交
			sourceConn.commit();
			sourceConn.setAutoCommit(true);
			
			LogManager.getLogger().info("來源庫【" + transTableName + "】刪除第" + curLine + "批資料成功，共" + count + "筆資料，耗時: " + (System.currentTimeMillis() - start) / 1000.0 + "s");
		} catch (Exception e) {
			LogManager.getLogger().error("來源庫【" + transTableName + "】刪除第" + curLine + "批資料失敗: " + e.getMessage());
			if (sourceConn != null) {
				sourceConn.rollback();
				sourceConn.setAutoCommit(true);
			}
		} finally {
			// close資源
			close(sourceConn, sourceStmt, null);
		}
	}
	
	/**
	 * 更新DB參數設定SQL組成 (SQL_SAFE_UPDATES、FOREIGN_KEY_CHECK、AUTOCOMMIT)
	 * 
	 * @param num
	 * @return
	 */
	private String updateDBSettingsStr(int num) {
		StringBuffer sbf = new StringBuffer();
		sbf.append("SET `SQL_SAFE_UPDATES` = " + num + "; ");
		sbf.append("SET `FOREIGN_KEY_CHECKS` = " + num + "; ");
		sbf.append("SET `AUTOCOMMIT` = " + num + "; ");
		return sbf.toString();
	}
	
	/**
	 * 更新system_config表內的backup_time
	 * 
	 * @param transTableName
	 * @throws JsonProcessingException 
	 * @throws JsonMappingException 
	 */
	public void updateSystemConfigBackupTime(String transTableName, String maxCreatedTime) {
		// 取system_config backup_time當下json字串
		String value = getBackupTimeValue();
		
		try {
			// 判斷value是否已事先寫入json字串
			ObjectMapper objectMapper = new ObjectMapper();
			if (TextHelper.fieldIsEmpty(value)) {
				HashMap<String, String> map = new HashMap<>();
				map.put(transTableName, maxCreatedTime);
				value = objectMapper.writeValueAsString(map);
				updateSystemConfigBackupTimeValue(transTableName, value);
			} else {
				HashMap<String, String> map = objectMapper.readValue(value, new TypeReference<HashMap<String, String>>() {});
				if (map.containsKey(transTableName)) {
					for (String tableName : map.keySet()) {
						if (tableName.equals(transTableName)) {
							map.replace(transTableName, maxCreatedTime);
						} 
					}
				} else {
					map.put(transTableName, maxCreatedTime);
				}
				value = objectMapper.writeValueAsString(map);
				updateSystemConfigBackupTimeValue(transTableName, value);
			}
		} catch (Exception e) {
			LogManager.getLogger().error(e.getMessage());
		}
	}
	
	/**
	 * 更新system_config表內backup_time各表時間戳
	 * 
	 * @param value (json字串)
	 */
	private void updateSystemConfigBackupTimeValue(String transTableName, String value) {
		// 來源庫資料
		Connection sourceConn = this.getSourceConn();
		PreparedStatement sourceStmt = null;
		
		try {
			String sql = "INSERT INTO `analytics`.`system_config` (`name`, `comment`, `value`, `status`) VALUES ('backup_time', '資料備份時間戳', '%s', '1') ON DUPLICATE KEY UPDATE `value` = VALUES(`value`);";
			sql = String.format(sql, value);
			sourceStmt = sourceConn.prepareStatement(sql);
			sourceStmt.executeUpdate();
		} catch (Exception e) {
			LogManager.getLogger().error(transTableName + "備份時間更新失敗");
		} finally {
			// close資源
			close(sourceConn, sourceStmt, null);
		}	
	}
	
	/**
	 * 查詢system_config內backup_time的value
	 * 
	 * @return
	 */
	private String getBackupTimeValue() {
		Connection sourceConn = this.getSourceConn();
		PreparedStatement sourceStmt = null;
		ResultSet sourceRs = null;
		String value = "";
		
		try {
			sourceStmt = sourceConn.prepareStatement("SELECT value FROM system_config WHERE name = 'backup_time';");
			sourceRs = sourceStmt.executeQuery();
			if (sourceRs.next()) {
				value = sourceRs.getString(1);
			}
		} catch (SQLException e) {
			LogManager.getLogger().error(e.getMessage());
		} finally {
			// close資源
			close(sourceConn, sourceStmt, sourceRs);
		}
		return value;
	}
	
	/**
	 * 關閉資料庫連線 
	 * 
	 * @param conn
	 * @param stmt
	 * @param rs
	 */
	public void close(Connection conn, Statement stmt, ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
            } catch (SQLException e) {
            	LogManager.getLogger().error(e.getMessage());
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
            	LogManager.getLogger().error(e.getMessage());
            }
        }
        if (conn != null) {
            try {
            	conn.close();
            } catch (SQLException e) {
            	LogManager.getLogger().error(e.getMessage());
            }
        }
	}
}
