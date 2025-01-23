package config

import (
	"flag"
	"os"
)

// legacy thing - used only for back-compatiblity command-line tools pg*.
type DBConfig struct {
	DBHost     string   `json:"db_host" yaml:"db_host"`
	DBPort     int      `json:"db_port" yaml:"db_port"`
	DBUser     string   `json:"db_user" yaml:"db_user"`
	DBPassword string   `json:"db_password" yaml:"db_password"`
	DBSchema   string   `json:"db_schema" yaml:"db_schema"`
	DBTables   []string `json:"db_tables" yaml:"db_tables"`
	DBFilter   []string `json:"db_filters" yaml:"db_filters"`
	DBName     string   `json:"db_name" yaml:"db_name"`
	Master     bool     `json:"master" yaml:"master"`
	TLSFile    string   `json:"tls_file" yaml:"tls_file"`
}

func (dc *DBConfig) HasTLS() bool {
	return dc.TLSFile != ""
}

type Config struct {
	DBConfig
	YtPath       string `json:"yt_path"`
	YtCluster    string `json:"yt_cluster"`
	YtSpec       string `json:"yt_spec"`
	YtToken      string `json:"yt_token"`
	MDBToken     string `json:"dbaas_token"`
	MDBClusterID string `json:"dbaas_cluster_id"`
}

type ArrayFlag []string

func (i *ArrayFlag) String() string {
	return "my string representation"
}

func (i *ArrayFlag) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func TryParseArgs() *Config {
	var config Config
	flag.StringVar(&config.YtPath, "yt-path", "", "YT Path where upload data")
	flag.StringVar(&config.YtCluster, "yt-cluster", "hahn", "YT Cluster where upload data")
	flag.StringVar(&config.YtSpec, "yt-spec", "", "YT Spec in json format")
	flag.StringVar(&config.YtToken, "yt-token", os.Getenv("YT_TOKEN"), "YT Token")
	flag.StringVar(&config.MDBClusterID, "dbaas-cluster-id", "", "MDB Cluster id from where data downloaded")
	flag.StringVar(&config.MDBToken, "dbaas-token", os.Getenv("DBAAS_TOKEN"), "MDB OAuth token see https://oauth.yandex-team.ru/authorize?response_type=token&client_id=8cdb2f6a0dca48398c6880312ee2f78d")
	flag.StringVar(&config.DBPassword, "db-password", os.Getenv("DB_PASSWORD"), "Postgresql password")
	flag.StringVar(&config.DBUser, "db-user", "", "Postgresql user")
	flag.StringVar(&config.DBSchema, "db-schema", "public", "DB schema")
	flag.StringVar(&config.DBName, "db-name", "", "Name of postgresql database")
	flag.StringVar(&config.DBHost, "db-host", "", "Name of db host, if not provided will take first not master host from MDB")
	flag.IntVar(&config.DBPort, "db-port", 6432, "Port, defaulted to pgbouncer port")
	flag.Var((*ArrayFlag)(&config.DBTables), "db-table", "List of table to be downloaded. If none provided will fetch all")
	flag.Var((*ArrayFlag)(&config.DBFilter), "db-filter", "List of table filters. For each table will be taken only one filter. Filter in format `Table(Predicate)`. Example `Event(created_at > '2018-01-01 and created_at < '2019-01-01')`")

	return &config
}
