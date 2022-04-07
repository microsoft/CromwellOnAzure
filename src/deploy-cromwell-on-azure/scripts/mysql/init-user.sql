CREATE USER 'cromwell'@'localhost' IDENTIFIED BY 'cromwell';
GRANT ALL PRIVILEGES ON cromwell_db.* TO 'cromwell'@'localhost' WITH GRANT OPTION;
{ReplaceMySqlLocalOrAzure}
GRANT ALL PRIVILEGES ON cromwell_db.* TO 'cromwell'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;