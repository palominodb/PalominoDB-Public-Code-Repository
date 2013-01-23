/* generate_binlogs.sql -
 * Cuts a set of binlogs to facilitate testing various types of events
 * in binlogs to ensure they're decoded properly.
 * Copyright (C) 2013 PalominoDB, Inc.
 * 
 * You may contact the maintainers at eng@palominodb.com.
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 *  
 *
 * If you add an event here, be sure to add validation
 * to <checkout>/util/t/files/binlogs/<binlog number>.txt
 * For example:
 *  binlog: mysql_binlog5141-bin.000001
 *  validation file: 000001.txt
 */

SET binlog_format='STATEMENT';
SET GLOBAL binlog_format='STATEMENT';
USE test;
DROP TABLE IF EXISTS binlog_test;
RESET MASTER; /* 000001 */
FLUSH LOGS; /* 000002 */
CREATE TABLE binlog_test (
       id INTEGER PRIMARY KEY AUTO_INCREMENT,
       x1 INTEGER,
       x2 VARCHAR(255),
       x3 TIMESTAMP,
       x4 DATETIME
) ENGINE=InnoDB;
FLUSH LOGS; /* 000003 */
INSERT INTO binlog_test (x2, x3) VALUES ('bob villa', NOW());
FLUSH LOGS; /* 000004 */
UPDATE binlog_test SET x4='2010-01-01 00:00:00' WHERE id=1;
FLUSH LOGS; /* 000005 */
TRUNCATE TABLE binlog_test;
FLUSH LOGS; /* 000006 */
LOAD DATA INFILE '../binlog_test_data.txt' INTO TABLE binlog_test;
FLUSH LOGS; /* 000007 */
SET @var_x1=4;
UPDATE binlog_test SET x2='frogs' WHERE id=@var_x1;
FLUSH LOGS; /* 000008 */
SET @var_x2='string value';
UPDATE binlog_test SET x2=@var_x2 WHERE id=@var_x1;
FLUSH LOGS; /* 000009 */
SET binlog_format='ROW';
SET GLOBAL binlog_format='ROW';
INSERT INTO binlog_test (x2, x3) VALUES ('popeye the sailor', NOW());
FLUSH LOGS; /* 000010 */
UPDATE binlog_test SET x2='olive oyl' WHERE id=LAST_INSERT_ID();
FLUSH LOGS; /* 000011 */
