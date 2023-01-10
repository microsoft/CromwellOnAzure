USE mysql;
DROP PROCEDURE IF EXISTS remove_change_lock;
DELIMITER $$
CREATE PROCEDURE remove_change_lock()
    BEGIN IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'cromwell_db' AND table_name = 'DATABASECHANGELOGLOCK')
        THEN UPDATE cromwell_db.DATABASECHANGELOGLOCK SET LOCKED = 0, LOCKGRANTED = null, LOCKEDBY = null WHERE ID = 1;
        END IF;
    END$$
DELIMITER ;
CALL remove_change_lock();
DROP PROCEDURE IF EXISTS remove_change_lock;
