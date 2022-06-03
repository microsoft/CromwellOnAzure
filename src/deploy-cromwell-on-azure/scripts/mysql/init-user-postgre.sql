CREATE USER {PostgreSqlUserLogin} WITH PASSWORD '{PostgreSqlUserPassword}';
GRANT ALL PRIVILEGES ON DATABASE {PostgreSqlDatabaseName} TO {PostgreSqlUserLogin};