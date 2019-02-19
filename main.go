package main

import "flag"

func main() {
	var sourceConfig sourceConfig
	var targetConfig targetConfig
	// Read all configurations
	configPath := flag.String("configPath", "", "Path for the configuration file")

	flag.Parse()

	ok, err := validateConfig(*configPath)
	if err != nil {
		return
	}

	if ok {
		// Parse source information
		// Parse target information
	}

	// Start parsing process
	start(sourceConfig, targetConfig)
}

func validateConfig(configPath string) (bool, error) { return true, nil }
func start(source, target interface{}) {
	// ping source configuration
	pinger(source)
	// ping target configuration
	pinger(target)
	// figure out batch sizes
	batcher(source)
	// poll for batch size
	poller(source, target)
}
func poller(source, target interface{}) {}
func batcher(source interface{}) bool   { return true }
func pinger(config interface{}) bool    { return true }

type sourceConfig struct {
	Type     string // DB , File
	SubType  string // dialect if database , file type if File
	FilePath string
	DBUser   string
	DBSchema string
	DBTable  string
	DBHost   string
	DBPort   string
	DBPass   string
	DBSql    string
}

type t argetConfig struct {
	Type     string // DB , File
	SubType  string // dialect if database , file type if File
	FilePath string
	DBUser   string
	DBSchema string
	DBTable  string
	DBHost   string
	DBPort   string
	DBPass   string
}
