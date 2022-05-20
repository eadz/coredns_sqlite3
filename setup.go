package coredns_sqlite3

import (
	"database/sql"
	"os"
	"strconv"
	"time"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
)

const (
	defaultTtl                = 360
	defaultMaxLifeTime        = 1 * time.Minute
	defaultMaxOpenConnections = 10
	defaultMaxIdleConnections = 10
	defaultZoneUpdateTime     = 10 * time.Minute
)

func init() {
	caddy.RegisterPlugin("sqlite3", caddy.Plugin{
		ServerType: "dns",
		Action:     setup,
	})
}

func setup(c *caddy.Controller) error {
	r, err := sqlite3Parse(c)
	if err != nil {
		return plugin.Error("sqlite3", err)
	}

	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		r.Next = next
		return r
	})

	return nil
}

func sqlite3Parse(c *caddy.Controller) (*CoreDNSSqlite3, error) {
	sqlite3 := CoreDNSSqlite3{
		TablePrefix: "coredns_",
		Ttl:         300,
	}
	var err error

	c.Next()
	if c.NextBlock() {
		for {
			switch c.Val() {
			case "dsn":
				if !c.NextArg() {
					return &CoreDNSSqlite3{}, c.ArgErr()
				}
				sqlite3.Dsn = c.Val()
			case "table_prefix":
				if !c.NextArg() {
					return &CoreDNSSqlite3{}, c.ArgErr()
				}
				sqlite3.TablePrefix = c.Val()
			case "max_lifetime":
				if !c.NextArg() {
					return &CoreDNSSqlite3{}, c.ArgErr()
				}
				var val time.Duration
				val, err = time.ParseDuration(c.Val())
				if err != nil {
					val = defaultMaxLifeTime
				}
				sqlite3.MaxLifetime = val
			case "max_open_connections":
				if !c.NextArg() {
					return &CoreDNSSqlite3{}, c.ArgErr()
				}
				var val int
				val, err = strconv.Atoi(c.Val())
				if err != nil {
					val = defaultMaxOpenConnections
				}
				sqlite3.MaxOpenConnections = val
			case "max_idle_connections":
				if !c.NextArg() {
					return &CoreDNSSqlite3{}, c.ArgErr()
				}
				var val int
				val, err = strconv.Atoi(c.Val())
				if err != nil {
					val = defaultMaxIdleConnections
				}
				sqlite3.MaxIdleConnections = val
			case "zone_update_interval":
				if !c.NextArg() {
					return &CoreDNSSqlite3{}, c.ArgErr()
				}
				var val time.Duration
				val, err = time.ParseDuration(c.Val())
				if err != nil {
					val = defaultZoneUpdateTime
				}
				sqlite3.zoneUpdateTime = val
			case "ttl":
				if !c.NextArg() {
					return &CoreDNSSqlite3{}, c.ArgErr()
				}
				var val int
				val, err = strconv.Atoi(c.Val())
				if err != nil {
					val = defaultTtl
				}
				sqlite3.Ttl = uint32(val)
			default:
				if c.Val() != "}" {
					return &CoreDNSSqlite3{}, c.Errf("unknown property '%s'", c.Val())
				}
			}

			if !c.Next() {
				break
			}
		}

	}

	db, err := sqlite3.db()
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	sqlite3.tableName = sqlite3.TablePrefix + "records"

	return &sqlite3, nil
}

func (handler *CoreDNSSqlite3) db() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", os.ExpandEnv(handler.Dsn))
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(handler.MaxLifetime)
	db.SetMaxOpenConns(handler.MaxOpenConnections)
	db.SetMaxIdleConns(handler.MaxIdleConnections)

	return db, nil
}
