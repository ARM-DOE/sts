package util

import (
    "errors"
    "math"
    "os/exec"
    "regexp"
    "runtime"
    "strconv"
    "strings"
    "sync"
    "time"
)

// DiskManager acts as the gateway between the STS and the data disk. A file must request a disk write
// which the disk manager may allow or block until disk space becomes available. DiskManager controls
// disk detection, formatting, mounting, and unmounting.
type DiskManager struct {
    current_disk      string     // A string that contains the path of the disk currently mounted.
    write_path        string     // The path that the disk should be mounted to and where files will be written.
    free_space        int64      // The amount of free space remaining on the disk in bytes.
    currently_writing int64      // The number of bytes that have been cleared to be written to the disk.
    disk_full         bool       // A boolean that is set to true when a file is too big to be written to disk.
    DISK_PADDING      int64      // The minimum number of bytes that should left empty on the disk.
    byte_lock         sync.Mutex // Mutex lock that must be obtained before altering the number of free bytes on the disk.
    last_space_used   int64      // How much space was used on the last disk. Used to check whether newly mounted disks are really new.
}

// CreateDiskManager creates a new instance of DiskManager and launches maintainDisk as a separate
// goroutine. This functionality is only enabled if the OS running STS is detected as Linux.
func CreateDiskManager(write_path string, mode string) *DiskManager {
    new_manager := DiskManager{}
    new_manager.DISK_PADDING = 1e7
    new_manager.byte_lock = sync.Mutex{}
    new_manager.write_path = write_path
    new_manager.last_space_used = -1
    if (runtime.GOOS == "linux" && mode == "auto") || mode == "true" {
        // Runtime is linux, or disk manager is enabled. Launch the monitoring goroutine.
        new_manager.ScanForDisk()
        go new_manager.maintainDisk()
    } else {
        // Not on linux, or disabled, disable most disk manager functionality by giving disk
        // very large storage size and a fake identifier.
        new_manager.current_disk = "not-linux-no-disk"
        new_manager.free_space = math.MaxInt64
    }
    return &new_manager
}

// maintainDisk is a method of DiskManager that runs on it's own goroutine, constantly checking
// disk status, unmounting the disk if it is full, and mounting a new disk if it is available.
func (manager *DiskManager) maintainDisk() {
    for {
        if !manager.HaveDisk() {
            // We don't have a disk mounted, try to find one.
            manager.ScanForDisk()
        } else {
            // We have a disk mounted, unmount if it gets full.
            if manager.disk_full {
                unmountDisk(manager.current_disk)
                manager.last_space_used, _ = getDiskSpace(manager.current_disk)
                manager.current_disk = ""
                manager.disk_full = false
            }
        }
        time.Sleep(time.Second * 10)
    }
}

// ScanForDisk checks calls prepareDisk to check if there is a valid disk to mount. If a disk is
// found, it is mounted and disk space levels are updated.
func (manager *DiskManager) ScanForDisk() bool {
    disk_path := prepareDisk(manager.write_path, manager.last_space_used)
    if len(disk_path) > 0 {
        // Found a disk, set it up.
        manager.current_disk = disk_path
        _, manager.free_space = getDiskSpace(disk_path)
        return true
    }
    return false
}

// CanWrite should be called before any data file is written to disk. Given the number of bytes
// that are requested for writing, CanWrite returns true if it is safe to write the file.
// Whether or not it is safe is determined by size left on disk & whether a disk is mounted.
func (manager *DiskManager) CanWrite(new_bytes int64) bool {
    if !manager.HaveDisk() {
        return false
    }
    manager.byte_lock.Lock()
    defer manager.byte_lock.Unlock()
    if manager.free_space-manager.currently_writing-manager.DISK_PADDING > new_bytes {
        return true
    }
    manager.disk_full = true
    return false
}

// WaitForWrite blocks until space is available to write to disk.
func (manager *DiskManager) WaitForWrite() {
    for {
        if manager.HaveDisk() && !manager.disk_full {
            return
        } else {
            time.Sleep(1 * time.Second)
        }
    }
}

// Writing should be called before writing a file to of size "bytes" to disk.
func (manager *DiskManager) Writing(bytes int64) {
    manager.byte_lock.Lock()
    manager.currently_writing += bytes
    manager.byte_lock.Unlock()
}

// DoneWriting should be called after writing a file of size "bytes" to disk.
func (manager *DiskManager) DoneWriting(bytes int64) {
    manager.byte_lock.Lock()
    manager.currently_writing -= bytes
    manager.free_space -= bytes
    manager.byte_lock.Unlock()
}

// HaveDisk returns true if there is a disk detected and mounted.
func (manager *DiskManager) HaveDisk() bool {
    if len(manager.current_disk) > 0 {
        return true
    } else {
        return false
    }
}

// prepareDisk attempts to find, format, and mount a disk based on regex patterns. If a disk is
// successfully mounted, the disk path is returned. If a disk is not successfully mounted, empty
// string is returned.
func prepareDisk(dest_path string, last_size int64) string {
    disk_label := "ARMDATAx4"
    disk_id := getDiskID()
    if len(disk_id) == 0 {
        return ""
    }
    disk_path := getDiskPath(disk_id)
    // Check to see if disk space is the same as the disk that was just unmounted.
    space_taken, _ := getDiskSpace(disk_path)
    if space_taken == last_size {
        // The space taken on this disk is the same as the disk we just unmounted.
        // It's quite likely it's the same disk, so don't mount it again.
        return ""
    }
    partition_path := disk_path + "1"
    if !isDiskMounted(dest_path, disk_path) {
        if !isDiskPartitioned(disk_path) {
            runCommand("parted", "-s", disk_path, "mkfs", "1", "FAT32")
        }
        if getDiskLabel(disk_path) != disk_label {
            runCommand("mkfs", "-t", "ext4", "-L", disk_label, partition_path)
        }
        mountDisk(partition_path, dest_path)
    } else {
        return disk_path
    }
    if isDiskMounted(dest_path, partition_path) {
        return disk_path
    }
    return ""
}

// runCommand runs a system command and returns the output as a string and any errors encountered.
func runCommand(executable_path string, args ...string) (string, error) {
    output, run_error := exec.Command(executable_path, args...).Output()
    if run_error != nil {
        return "", run_error
    }
    return string(output), nil
}

// getDiskSpace uses a disk path (something like /dev/sdc) to check how much space remains on the disk.
// It returns two int64 integers (space used, space free).
func getDiskSpace(search_disk string) (int64, int64) {
    disk_output, err := runCommand("df", "-k")
    if err != nil {
        return 0, 0
    }
    split_disks := strings.Split(disk_output, "\n")
    for _, disk := range split_disks {
        if strings.Contains(disk, search_disk) {
            disk_pattern := regexp.MustCompile(`[^ A-z]\d+`)
            matches := disk_pattern.FindAllString(disk, -1)
            if len(matches) < 3 {
                return 0, 0
            }
            space_used, used_err := strconv.ParseInt(matches[1], 10, 64)
            space_free, free_err := strconv.ParseInt(matches[2], 10, 64)
            if used_err != nil || free_err != nil {
                return 0, 0
            }
            return space_used * 1024, space_free * 1024
        }
    }
    return 0, 0
}

// getDiskID uses regular expressions to parse the output of system commands and find a disk with
// the correct product number, manufacturer, ect. If a disk that matches these criteria is found,
// the raw path is returned. If one isn't found, empty string is returned.
func getDiskID() string {
    usb_output, err := runCommand("systool", "-b", "usb", "-v")
    if err != nil {
        return ""
    }
    id_pattern := regexp.MustCompile(`\{?idProduct\}?\s*=\s*"[a5][b01][236][0134]"`)
    device_path_pattern := regexp.MustCompile(`Device path\s*=\s*"([^"]+)"`)
    match := id_pattern.FindString(usb_output)
    if len(match) == 0 {
        return ""
    }
    before_id := strings.Split(usb_output, match)[0]
    device_paths := device_path_pattern.FindAllString(before_id, -1)
    disk_path := strings.Split(device_paths[len(device_paths)-1], `"`)[1]
    return disk_path
}

// getDiskPath uses regular expressions to parse the output of system commands and find the disk path
// that corresponds to the given the disk ID. This disk path can then be used for mounting, free space
// checking, ect.
func getDiskPath(disk_id string) string {
    // Do lsscsi lookup to find path to use in "fdisk -l" lookup.
    lss_output, err := runCommand("lsscsi", "-v")
    if err != nil {
        return ""
    }
    // Parse output from lsscsi
    lss_split := strings.Split(lss_output, "\n")
    for index, item := range lss_split {
        if strings.Contains(item, disk_id) {
            id_line := strings.Split(lss_split[index-1], " ")
            return id_line[len(id_line)-2]
        }
    }
    return ""
}

// getPartitionPath parses system commands to get the path of the first partition on the given disk.
// Currently unused in the STS.
func getPartitionPath(disk_path string) string {
    // Parse output from "fdisk -l"
    fdisk_output, fdisk_err := runCommand("fdisk", "-l")
    if fdisk_err != nil {
        // Couldn't execute fdisk, probably not root
        return ""
    }
    fdisk_pattern := regexp.MustCompile(disk_path + `[^:].*`)
    disk_line := fdisk_pattern.FindString(fdisk_output)
    partition_path := strings.Split(disk_line, " ")[0]
    return partition_path
}

// getDiskLabel returns the label of the specified disk.
// It is used to check if a disk is formatted/partitioned.
func getDiskLabel(disk_path string) string {
    e2label_output, e2label_err := runCommand("e2label", disk_path)
    if e2label_err != nil {
        return ""
    }
    return e2label_output
}

// isDiskMounted parses system commands to see if, given the mount location and path of the
// partition, the disk is actually mounted.
func isDiskMounted(mount_directory string, partition_path string) bool {
    df_output, df_err := runCommand("df", "-k", mount_directory)
    if df_err != nil {
        return false
    }
    if strings.Contains(df_output, partition_path) {
        return true
    }
    return false
}

// isDiskPartitioned parses system commands to see if the disk is currently partitioned.
func isDiskPartitioned(disk_path string) bool {
    parted_output, _ := runCommand("parted", "-s", disk_path, "print")
    if strings.Contains(parted_output, "Could not stat device") {
        return false
    }
    parted_pattern := regexp.MustCompile(`\n\s*1`)
    is_parted := parted_pattern.MatchString(parted_output)
    return is_parted
}

// mountDisk uses system commands to mount a partition path at a destination path.
func mountDisk(partition_path string, dest_path string) error {
    mount_output, mount_err := runCommand("mount", "-t", "ext3,ext4", partition_path, dest_path)
    if strings.Contains(mount_output, "is already mounted on") {
        return errors.New(partition_path + " is already mounted")
    }
    return mount_err
}

// unmountDisk takes a disk path and uses system commands to unmount the disk at that path.
func unmountDisk(disk_path string) error {
    unmount_output, mount_err := runCommand("umount", disk_path)
    if strings.Contains(unmount_output, "not mounted") {
        return errors.New(disk_path + " is not mounted")
    }
    return mount_err
}
