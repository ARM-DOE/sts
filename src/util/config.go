package util

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
)

// Config is the struct that all values from the configuration file are loaded into when it is parsed.
type Config struct {
	// Static configuration values from file - require restart to change
	Input_Directory      string
	Sender_Threads       int
	Sender_Name          string
	Cache_File_Name      string
	Cache_Write_Interval int64
	Disk_Path            string
	Bin_Store            string
	Output_Directory     string
	Server_Port          string
	Receiver_Name        string
	Receiver_Address     string
	Staging_Directory    string
	Logs_Directory       string
	TLS                  bool
	Bin_Timeout          int
	Server_SSL_Cert      string
	Server_SSL_Key       string
	Client_SSL_Cert      string
	Client_SSL_Key       string
	Disk_Switching       string
	Dynamic              DynamicValues
	// Internal config values
	file_name      string
	should_reload  bool
	should_restart bool
}

// DynamicValues are the values that can be changed without needing a restart.
// Every Dynamic value needs a get method and must be used in a safe-to-reload way
// Changes to dynamic values will not trigger a restart, changes to any other values will.
type DynamicValues struct {
	Tags        map[string]*ConfigTagData // The values parsed directly from the config, the initial stage. Shouldn't be used anywhere in the program. Any changes to fields here will need to be reflected in ConvertParsedTag().
	parsed_tags map[string]*TagData       // Tags that have been reflected and parsed to contain correct data. Should be accessed via config.Tags().
	Compression bool
	Bin_Size    int
}

// ConfigTagData contains the config values for each tag, every field must be a string so it can
// be tested against the default config values.
type ConfigTagData struct {
	Priority         string
	Transfer_Method  string
	Sort             string
	Delete_On_Verify string
	Last_File        string // Name of the last file sent from this tag. Used for linked list sorting on receiving end.
}

// TagData is the parsed version of ConfigTagData. Data types have been converted to their real types via ConvertParseTag()
type TagData struct {
	Priority         int
	Transfer_Method  string
	Sort             string
	Delete_On_Verify bool
	Last_File        string // Name of the last file sent from this tag. Used for linked list sorting on receiving end.
}

func (tag *TagData) TransferMethod() string {
	return strings.ToLower(tag.Transfer_Method)
}

func (tag *TagData) SetLastFile(last_file string) {
	if tag.Sort == "none" {
		// Don't set the last file for tags that don't have ordering configured.
		return
	}
	tag.Last_File = last_file
}

func (tag *TagData) LastFile() string {
	return tag.Last_File
}

// parseConfig parses the config.yaml file and returns the parsed results as an instance of the Config struct.
func ParseConfig(file_name string) (*Config, error) {
	var loaded_config Config
	var abs_path string
	if !filepath.IsAbs(file_name) {
		var abs_err error
		abs_path, abs_err = filepath.Abs(file_name)
		if abs_err != nil {
			return &Config{}, abs_err
		}
	} else {
		abs_path = file_name
	}
	config_fi, config_err := ioutil.ReadFile(abs_path)
	if config_err != nil {
		LogError("Configuration file not found: ", file_name)
		os.Exit(1)
	}
	err := yaml.Unmarshal(config_fi, &loaded_config)
	if err != nil {
		LogError(err.Error())
		os.Exit(1)
	}
	loaded_config.ParseTags()
	loaded_config.file_name = abs_path
	return &loaded_config, nil
}

func initPath(path *string, isdir bool) {
	var err error
	if !filepath.IsAbs(*path) {
		root := GetRootPath()
		*path, err = filepath.Abs(JoinPath(root, *path))
		if err != nil {
			LogError("Failed to initialize: ", *path, err.Error())
		}
	}
	pdir := *path
	if !isdir {
		pdir = filepath.Dir(pdir)
	}
	_, err = os.Stat(pdir)
	if os.IsNotExist(err) {
		LogDebug("CONFIG Make Path:", pdir)
		os.MkdirAll(pdir, os.ModePerm)
	}
}

// InitPaths iteratores over the configuration options that represent either a file or directory
// on disk and initializes it.
func (config *Config) InitPaths() {
	initPath(&config.Cache_File_Name, false)
	initPath(&config.Bin_Store, true)
	initPath(&config.Input_Directory, true)
	initPath(&config.Output_Directory, true)
	initPath(&config.Staging_Directory, true)
	initPath(&config.Logs_Directory, true)
}

// ParseTags iterates over every ConfigTagData parsed from the config and sets any empty values
// to the value found in the default ConfigTagData. After this, every ConfigTagData is converted
// to a TagData struct.
func (config *Config) ParseTags() {
	// Use reflection to set all empty-string tag fields to the value of the default field.
	default_tag := config.Dynamic.Tags["DEFAULT"]
	for tag_string, config_tag := range config.Dynamic.Tags {
		if tag_string == "DEFAULT" {
			continue
		}
		field_count := reflect.TypeOf(config_tag).Elem().NumField()
		weird_slice_arg := []int{0}
		for field_index := 0; field_index < field_count; field_index++ {
			weird_slice_arg[0] = field_index
			tag_field := reflect.TypeOf(config_tag).Elem().FieldByIndex(weird_slice_arg)
			tag_value := reflect.Indirect(reflect.ValueOf(config_tag)).FieldByName(tag_field.Name)
			default_field := reflect.TypeOf(default_tag).Elem().FieldByIndex(weird_slice_arg)
			default_value := reflect.Indirect(reflect.ValueOf(default_tag)).FieldByName(default_field.Name)
			if tag_value.String() == "" {
				tag_value.SetString(default_value.String())
			}
		}
	}
	// Convert ConfigTagData to TagData.
	config.Dynamic.parsed_tags = make(map[string]*TagData)
	for tag_string, config_tag := range config.Dynamic.Tags {
		config.Dynamic.parsed_tags[tag_string] = ConvertParsedTag(config_tag)
	}
}

// ConvertParsedTag takes a ConfigTagData and converts it to a normal TagData, parsing all
// string values to their proper types. THIS FUNCTION WILL NEED TO BE UPDATED IF THE VALUES OF
// ConfigTagData CHANGE.
func ConvertParsedTag(config_tag *ConfigTagData) *TagData {
	tag_data := &TagData{}
	tag_data.Delete_On_Verify, _ = strconv.ParseBool(config_tag.Delete_On_Verify)
	tag_data.Priority, _ = strconv.Atoi(config_tag.Priority)
	tag_data.Sort = config_tag.Sort
	tag_data.Transfer_Method = config_tag.Transfer_Method
	return tag_data
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
func (config *Config) Tags() map[string]*TagData {
	return config.Dynamic.parsed_tags
}

func (config *Config) Compression() bool {
	return config.Dynamic.Compression
}

func (config *Config) BinSize() int {
	return config.Dynamic.Bin_Size
}

func (config *Config) Protocol() string {
	if config.TLS {
		return "https"
	}
	return "http"
}

// EditConfig can be used as an http.HandleFunc. It should be used to handle a page named
// "/edit_config.go" for correct interoperability with EditConfigInterface(). It accepts
// a parameter named "config" that should contain a full copy of the config, with any
// changed values. If the YAML not parseable, the server will respond with a 400 (Bad Request).
func (config *Config) EditConfig(w http.ResponseWriter, r *http.Request) {
	new_config := r.FormValue("config")
	if len(new_config) < 1 {
		fmt.Fprint(w, http.StatusBadRequest)
		return
	}
	// Check if yaml is valid
	var test_config Config
	unmarshal_err := yaml.Unmarshal([]byte(new_config), &test_config)
	if unmarshal_err != nil {
		fmt.Fprintf(w, "%d: %s", http.StatusBadRequest, unmarshal_err.Error())
		return
	}
	// Config is valid, write to file.
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
func (config *Config) StaticDiff(old_config *Config) bool {
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

// EditConfigInterface can be used as an http.HandleFunc. It should be used on a page named
// "/editor.go" for correct interoperability with EditConfig(). It sends a pretty HTML
// interface with an embedded javascript text editor (ACE). The text editor is pre-filled with
// the current contents of the config. When the user clicks the "save" button in the
// HTML form, the contents of the editor are posted to "/edit_config.go" on the same server under
// the key "config".
func (config *Config) EditConfigInterface(w http.ResponseWriter, r *http.Request) {
	config_contents, read_err := ioutil.ReadFile(config.file_name)
	if read_err != nil {
		fmt.Fprint(w, read_err.Error())
	} else {
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
}
