/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.epinions.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GetReviewsByUser extends Procedure {

    public final SQLStmt getReviewUser = new SQLStmt(
            "SELECT * FROM review r, useracct u WHERE u.u_id = r.u_id AND r.u_id=? " +
                    "ORDER BY rating LIMIT 10;"
    );

    public void run(Connection conn, long uid) throws SQLException {
        String t = "";
	boolean printT = true;
	int count = 0;
	try (PreparedStatement stmt = this.getPreparedStatement(conn, getReviewUser)) {
            stmt.setLong(1, uid);
            try (ResultSet r = stmt.executeQuery()) {
                while (r.next()) {
                    if (count == 0) {
			    t += String.format("%s:%d", "rating", r.getInt(1));
		    } else {
			    t += String.format(",%s:%d", "rating", r.getInt(1));
		    }
//		    t += String.format(",%s:%d", "rating", r.getInt(2));
//		    t += String.format(",%s:%d", "rating", r.getInt(3));
//		    if (r.getInt(4) != 0) {
//			    t += String.format(",%s:%d", "rating", r.getInt(4));
//		    }
//		    if (r.getInt(5) != 0) {
//			    t += String.format(",%s:%d", "rating", r.getInt(5));
//		    }
//		    t += String.format(",%s:%d", "useracct", r.getInt(6));
//		    t += String.format(",%s:%d", "useracctu", r.getInt(6));
			continue;
                }
            }
        }
	if (printT) {
	if (t.length() > 0) {
		t = "u;" + t;
	}
	System.out.println(t);
	}
    }
}
