SELECT 'CREATE DATABASE nextmini OWNER pgusr'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'nextmini'
)\gexec

GRANT ALL PRIVILEGES ON DATABASE nextmini TO pgusr;

ALTER DATABASE nextmini OWNER TO pgusr;
