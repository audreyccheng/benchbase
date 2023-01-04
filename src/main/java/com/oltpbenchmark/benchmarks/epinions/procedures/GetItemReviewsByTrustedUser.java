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

public class GetItemReviewsByTrustedUser extends Procedure {

    //FIXME: CARLO, does this make sense?
    public final SQLStmt getReview = new SQLStmt(
            "SELECT * FROM review r WHERE r.i_id=?"
    );

    public final SQLStmt getTrust = new SQLStmt(
            "SELECT * FROM trust t WHERE t.source_u_id=?"
    );

    public void run(Connection conn, long iid, long uid) throws SQLException {
        String t = "";
	boolean printT = true;
	int count = 0;
	try (PreparedStatement stmt = this.getPreparedStatement(conn, getReview)) {
            stmt.setLong(1, iid);
            try (ResultSet r = stmt.executeQuery()) {
                while (r.next()) {
                    if (count == 0) {
			    t += String.format("%s:%d", "rating", r.getInt(1));
			    count++;
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
	    	    continue;
                }
            }
        }
        try (PreparedStatement stmt = this.getPreparedStatement(conn, getTrust)) {
            stmt.setLong(1, uid);
            try (ResultSet r = stmt.executeQuery()) {
                while (r.next()) {
                    if (count == 0) {
			    t += String.format("%s:%d", "trust", r.getInt(1));
			    count++;
		    } else {
			    t += String.format(",%s:%d", "trust", r.getInt(1));
		    }
//		    t += String.format(",%s:%d", "trust", r.getInt(2));
//		    t += String.format(",%s:%d", "trust", r.getInt(3));
//		    if (r.getTimestamp(4) != null) {
//			    t += String.format(",%s:%d", "trust", r.getTimestamp(4).getTime());
//		    }
		    continue;
                }
            }
        }
	if (printT) {
	if (t.length() > 0) {
		t = "t;" + t;
	}
	System.out.println(t);
	}
    }

}
