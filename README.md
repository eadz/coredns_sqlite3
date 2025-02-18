# Sqlite3

Sqlite3 backend for CoreDNS

## Name
sqlite3 - Sqlite3 backend for CoreDNS

## Description

This plugin uses Sqlite3 as a backend to store DNS records. These will then can served by CoreDNS. The backend uses a simple, single table data structure that can be shared by other systems to add and remove records from the DNS server. As there is no state stored in the plugin, the service can be scaled out by spinning multiple instances of CoreDNS backed by the same database.

## Syntax
```
sqlite3 {
    dsn DSN
    [table_prefix TABLE_PREFIX]
    [max_lifetime MAX_LIFETIME]
    [max_open_connections MAX_OPEN_CONNECTIONS]
    [max_idle_connections MAX_IDLE_CONNECTIONS]
    [ttl DEFAULT_TTL]
    [zone_update_interval ZONE_UPDATE_INTERVAL]
}
```

- `dsn` DSN for Sqlite3 as per https://github.com/mattn/go-sqlite3 examples. You can use `$ENV_NAME` format in the DSN, and it will be replaced with the environment variable value.
- `table_prefix` Prefix for the Sqlite3 tables. Defaults to `coredns_`.
- `max_lifetime` Duration (in Golang format) for a SQL connection. Default is 1 minute.
- `max_open_connections` Maximum number of open connections to the database server. Default is 10.
- `max_idle_connections` Maximum number of idle connections in the database connection pool. Default is 10.
- `ttl` Default TTL for records without a specified TTL in seconds. Default is 360 (seconds)
- `zone_update_interval` Maximum time interval between loading all the zones from the database. Default is 10 minutes.

## Supported Record Types

A, AAAA, CNAME, SOA, TXT, NS, MX, CAA and SRV. This backend doesn't support AXFR requests. It also doesn't support wildcard records yet.

## Setup (as an external plugin)

Add this as an external plugin in `plugin.cfg` file: 

```
mysql:github.com/cloud66-oss/coredns_mysql
```

then run
 
```shell script
$ go generate
$ go build
```

Add any required modules to CoreDNS code as prompted.

## Database Setup
This plugin doesn't create or migrate database schema for its use yet. To create the database and tables, use the following table structure (note the table name prefix):

```sql
CREATE TABLE `coredns_records` (
    `id` INT NOT NULL AUTO_INCREMENT,
	`zone` VARCHAR(255) NOT NULL,
	`name` VARCHAR(255) NOT NULL,
	`ttl` INT DEFAULT NULL,
	`content` TEXT,
	`record_type` VARCHAR(255) NOT NULL,
	PRIMARY KEY (`id`)
) ENGINE = INNODB AUTO_INCREMENT = 6 DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
```

## Record setup
Each record served by this plugin, should belong to the zone it is allowed to server by CoreDNS. Here are some examples:

```sql
-- Insert batch #1
INSERT INTO coredns_records (zone, name, ttl, content, record_type) VALUES
('example.org.', 'foo', 30, '{"ip": "1.1.1.1"}', 'A'),
('example.org.', 'foo', '60', '{"ip": "1.1.1.0"}', 'A'),
('example.org.', 'foo', 30, '{"text": "hello"}', 'TXT'),
('example.org.', 'foo', 30, '{"host" : "foo.example.org.","priority" : 10}', 'MX');
```

These can be queries using `dig` like this:

```shell script
$ dig A MX foo.example.org 
```

### Acknowledgements and Credits
This plugin, is inspired by https://github.com/wenerme/coredns-pdsql and https://github.com/arvancloud/redis

### Development 
To develop this plugin further, make sure you can compile CoreDNS locally and get this repo (`go get github.com/cloud66-oss/coredns_mysql`). You can switch the CoreDNS mod file to look for the plugin code locally while you're developing it:

Put `replace github.com/cloud66-oss/coredns_mysql => LOCAL_PATH_TO_THE_SOURCE_CODE` at the end of the `go.mod` file in CoreDNS code. 

Pull requests and bug reports are welcome!

