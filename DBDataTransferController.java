package com.hyena.backstage.datatransfer;

import java.sql.SQLException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * DB資料轉移 API
 *
 * @author brian.chang
 * @version 2022/04/28 
 *
 */
@RestController
@RequestMapping("/api/data/trans")
public class DBDataTransferController {
	
	@RequestMapping(method = RequestMethod.GET)
	public void doGet(HttpServletRequest request, HttpServletResponse response) {
		response.setCharacterEncoding("UTF-8");
		DBDataTransfer test = new DBDataTransfer();
		String[] transTables = DBDataTransfer.transTables;
		int loopTimes = 0; // 共幾批
		int tableDataCount = 0; // Table資料總數
		int offset = 10000; // 每批資料10000筆
		long start = System.currentTimeMillis(); // 取當下時間 (計算耗時時間)
		String maxCreatedTime = ""; // 第X批資料最大時間
		
		try {
			outer:for (int i = 0;i < transTables.length; i++) {
				tableDataCount = test.getTableDataCount(transTables[i]);
				loopTimes = (int) Math.ceil((double)tableDataCount / (double)offset);
				if (loopTimes != 0) {
					LogManager.getLogger().info("【" + transTables[i] + "】轉移次數分為" + loopTimes + "次");
				}

				int curLine = 0; // 此次批數
				for (int loopN = 0; loopN < loopTimes; loopN++) {
					curLine++;
					LogManager.getLogger().info("【" + transTables[i] + "】正在轉移第" + curLine + "批資料");
					maxCreatedTime = test.transfer(transTables[i], tableDataCount, offset, curLine);
					if (DBDataTransfer.isTransfer && tableDataCount != 0) {
						// 刪除此批來源庫資料
						test.deleteSourceTableData(transTables[i], offset, curLine);
						// 更新system_config備份資料時間
						test.updateSystemConfigBackupTime(transTables[i], maxCreatedTime);
					} else if (!DBDataTransfer.isTransfer) {
						LogManager.getLogger().error("【" + transTables[i] + "】資料轉移失敗");
						break outer;
					}
				}
				if (tableDataCount != 0) {
					LogManager.getLogger().info("【" + transTables[i] + "】資料轉移完畢，共耗時: " + (System.currentTimeMillis() - start) / 1000.0 + "s");
				}
			}
			LogManager.getLogger().info("log表資料轉移完畢，總耗時: " + (System.currentTimeMillis() - start) / 1000.0 + "s");
			response.getWriter().write("資料轉移成功！");
		} catch (SQLException e) {
			LogManager.getLogger().error(e.getMessage());
	    } catch (Exception e) {
			LogManager.getLogger().error(e.getMessage());
		}
	}
}
