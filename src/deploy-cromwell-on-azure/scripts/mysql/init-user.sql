CREATE USER '{MySqlUserLogin}'@'localhost' IDENTIFIED BY '{MySqlUserPassword}';
GRANT ALL PRIVILEGES ON {MySqlDatabaseName}.* TO '{MySqlUserLogin}'@'localhost' WITH GRANT OPTION;
CREATE USER '{MySqlUserLogin}'@'%' IDENTIFIED BY '{MySqlUserPassword}';
GRANT ALL PRIVILEGES ON {MySqlDatabaseName}.* TO '{MySqlUserLogin}'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;