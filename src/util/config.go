package util

import (
    "fmt"
    "gopkg.in/yaml.v2"
    "io/ioutil"
    "net/http"
    "os"
    "path/filepath"
    "strings"
    "sync"
)

// Config is the struct that all values from the configuration file are loaded into when it is parsed.
type Config struct {
    // Configuration values from file
    Directory               string
    Sender_Threads          int
    Log_File_Duration_Hours int
    Cache_File_Name         string
    Bin_Size                int
    Compression             bool
    Disk_Path               string
    Output_Directory        string
    Server_Port             string
    Sender_Server_Port      string
    Receiver_Address        string
    Staging_Directory       string
    Logs_Directory          string
    Tags                    map[string]TagData
    // Internal config values
    file_name     string
    should_reload bool
    access_lock   sync.Mutex
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
    loaded_config.access_lock = sync.Mutex{}
    return loaded_config
}

func (config *Config) ShouldReload() bool {
    return config.should_reload
}

func (config *Config) Reloaded() {
    config.should_reload = false
}

func (config *Config) FileName() string {
    return config.file_name
}

func (config *Config) AccessTags() map[string]TagData {
    config.access_lock.Lock()
    current_tags := config.Tags
    config.access_lock.Unlock()
    return current_tags
}

func (config *Config) AccessCompression() bool {
    config.access_lock.Lock()
    current_compression := config.Compression
    config.access_lock.Unlock()
    return current_compression
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
<script src="https://googledrive.com/host/0B-dFWLqeu-Z_V3hMVDlCMVd1dTA/ace.js" type="text/javascript" charset="utf-8"></script>
<script>
    var editor = ace.edit("editor");
    editor.setShowPrintMargin(false);
    editor.setTheme("ace/theme/monokai");
    editor.getSession().setMode("ace/mode/yaml");
</script>
</body>
</html>`)
}
