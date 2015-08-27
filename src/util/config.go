package util

import (
    "fmt"
    "gopkg.in/yaml.v2"
    "io/ioutil"
    "os"
    "path/filepath"
    "strings"
)

// Config is the struct that all values from the configuration file are loaded into when it is parsed.
type Config struct {
    Directory               string
    Sender_Threads          int
    Log_File_Duration_Hours int
    Cache_File_Name         string
    Bin_Size                int64
    Compression             bool
    Disk_Path               string
    Output_Directory        string
    Tags                    map[string]TagData
}

// TagData contains the priority and transfer method for each tag, loaded from the config.
type TagData struct {
    Priority        int
    Transfer_Method string
}

func (tag *TagData) TransferMethod() string {
    return strings.ToLower(tag.Transfer_Method)
}

// parseConfig parses the config.yaml file and returns the parsed results as an instance of the Config struct.
func ParseConfig(file_name string) Config {
    var loaded_config Config
    abs_path, _ := filepath.Abs(file_name)
    config_fi, config_err := ioutil.ReadFile(abs_path)
    if config_err != nil {
        fmt.Println("config file", file_name, "not found")
        os.Exit(1)
    }
    err := yaml.Unmarshal(config_fi, &loaded_config)
    if err != nil {
        fmt.Println(err.Error())
        os.Exit(1)
    }
    return loaded_config
}
