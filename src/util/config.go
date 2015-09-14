package util

import (
    "fmt"
    "gopkg.in/yaml.v2"
    "io/ioutil"
    "net/http"
    "os"
    "path/filepath"
    "strings"
)

// Config is the struct that all values from the configuration file are loaded into when it is parsed.
type Config struct {
    // Static configuration values from file - require restart to change
    Directory               string
    Sender_Threads          int
    Log_File_Duration_Hours int
    Cache_File_Name         string
    Disk_Path               string
    Output_Directory        string
    Server_Port             string
    Sender_Server_Port      string
    Receiver_Address        string
    Staging_Directory       string
    Logs_Directory          string
    Dynamic                 DynamicValues
    // Internal config values
    file_name      string
    should_reload  bool
    should_restart bool
}

// DynamicValues are the values that can be changed without needing a restart.
// Every Dynamic value needs a get method and must be used in a safe-to-reload way
// Changes to dynamic values will not trigger a restart, changes to any other values will.
type DynamicValues struct {
    Tags        map[string]TagData
    Compression bool
    Bin_Size    int
}

// TagData contains the priority and transfer method for each tag, loaded from the config.
type TagData struct {
    Priority        int
    Transfer_Method string
    Sort            string
    Delete_On_Send  bool
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
    loaded_config.file_name = abs_path
    return loaded_config
}

func (config *Config) ShouldReload() bool {
    return config.should_reload
}

// Reloaded should be called after the config has been successfully reloaded.
func (config *Config) Reloaded() {
    config.should_reload = false
}

func (config *Config) FileName() string {
    return config.file_name
}

// AccessTags is the access method for
func (config *Config) Tags() map[string]TagData {
    return config.Dynamic.Tags
}

func (config *Config) Compression() bool {
    return config.Dynamic.Compression
}

func (config *Config) BinSize() int {
    return config.Dynamic.Bin_Size
}

func (config *Config) EditConfig(w http.ResponseWriter, r *http.Request) {
    new_config := r.FormValue("config")
    if len(new_config) < 1 {
        fmt.Fprint(w, http.StatusBadRequest)
        return
    }
    temp_config := config.file_name + ".tmp"
    conf_fi, _ := os.Create(temp_config)
    conf_fi.Write([]byte(new_config))
    conf_fi.Close()
    os.Rename(temp_config, config.file_name)

    if strings.HasSuffix(r.Referer(), "editor.go") {
        // If the editor sent the user, redirect them back to the editor.
        http.Redirect(w, r, "/editor.go", 301)
    }
    config.should_reload = true
}

// StaticDiff takes two configs and checks if their static variables are the same by nulling
// all dynamic/runtime variables in the config. Could potentially need updates if new variables
// are added to Config.
func (config Config) StaticDiff(old_config Config) bool {
    static_values_changed := false
    // Set everything but static values to default
    config.Dynamic = DynamicValues{}
    config.should_reload = false
    config.should_restart = false
    old_config.Dynamic = DynamicValues{}
    old_config.should_reload = false
    old_config.should_restart = false
    // Test if two string representations of config are equal
    if !(fmt.Sprintf("%v", config) == fmt.Sprintf("%v", old_config)) {
        static_values_changed = true
    }
    return static_values_changed
}

func (config *Config) EditConfigInterface(w http.ResponseWriter, r *http.Request) {
    config_contents, _ := ioutil.ReadFile(config.file_name)
    fmt.Fprint(w,
        `
<!DOCTYPE html>
<html lang="en">
<head>
<title>STS Config Editor</title>
<style>
html { visibility:hidden; }
</style>
<style type="text/css" media="screen">
    #editor { 
        position: center;
        height: 450px;
        width: 60%;
        left: 20%;
    }
</style>

</head>
<body bgcolor="#2F3129" onload=loaded()>
</br>
<script>
function save() {
    document.getElementById("save_button").disabled = true;
    var editor = ace.edit("editor");
    var new_config = editor.getValue();
    document.getElementById("editor_contents").value = new_config;
    document.getElementById("save_form").submit();
    document.getElementById("save_button").disabled = false;
}

function loaded() {
    document.getElementsByTagName("html")[0].style.visibility = "visible";
}
</script>
<form id="save_form" action="edit_config.go" method="POST">
<input type="hidden" name="config" id="editor_contents" value="">
<div id="editor" visibility="hidden">`+string(config_contents)+`</div>
</br><center><input type="submit" id="save_button" onClick=save();return false; value="Save"></center></form>
<script src="https://engineering.arm.gov/~dohnalek/ace.js" type="text/javascript" charset="utf-8"></script>
<script>
    var editor = ace.edit("editor");
    editor.setShowPrintMargin(false);
    editor.setTheme("ace/theme/monokai");
    editor.getSession().setMode("ace/mode/yaml");
</script>
</body>
</html>`)
}
