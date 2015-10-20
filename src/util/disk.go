package util

import (
    "errors"
    "os/exec"
    "regexp"
    "strconv"
    "strings"
)

func prepareDisk(dest_path string) error {
    disk_label := "ARMDATAx4"
    disk_id := getDiskID()
    disk_path := getDiskPath(disk_id)
    partition_path := disk_path + "1"
    if !isDiskMounted(dest_path, disk_path) {
        if !isDiskPartitioned(disk_path) {
            runCommand("parted", "-s", disk_path, "mkfs", "1", "FAT32")
        }
        if getDiskLabel(disk_path) != disk_label {
            runCommand("mkfs", "-t", "ext4", "-L", disk_label, partition_path)
        }
        mountDisk(partition_path, dest_path)
    }
    if isDiskMounted(dest_path, partition_path) {
        return nil
    }
    return errors.New("Disk failed to mount")
}

func runCommand(executable_path string, args ...string) (string, error) {
    output, run_error := exec.Command(executable_path, args...).Output()
    if run_error != nil {
        return "", run_error
    }
    return string(output), nil
}

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

func getDiskLabel(disk_path string) string {
    e2label_output, e2label_err := runCommand("e2label", disk_path)
    if e2label_err != nil {
        return ""
    }
    return e2label_output
}

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

func isDiskPartitioned(disk_path string) bool {
    parted_output, _ := runCommand("parted", "-s", disk_path, "print")
    if strings.Contains(parted_output, "Could not stat device") {
        return false
    }
    parted_pattern := regexp.MustCompile(`\n\s*1`)
    is_parted := parted_pattern.MatchString(parted_output)
    return is_parted
}

func mountDisk(partition_path string, dest_path string) error {
    mount_output, mount_err := runCommand("mount", "-t", "ext3,ext4", partition_path, dest_path)
    if strings.Contains(mount_output, "is already mounted on") {
        return errors.New(partition_path + " is already mounted")
    }
    return mount_err
}

func unmountDisk(disk_id string) error {
    unmount_output, mount_err := runCommand("umount", disk_id)
    if strings.Contains(unmount_output, "not mounted") {
        return errors.New(disk_id + " is not mounted")
    }
    return mount_err
}
