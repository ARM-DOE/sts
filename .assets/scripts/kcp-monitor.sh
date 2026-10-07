#!/bin/bash

set -u

interval=5
duration=0
port=1992
if [[ ! -r /proc/stat || ! -r /proc/net/dev ]]; then
    echo "This collector requires Linux /proc." >&2
    exit 1
fi
if [[ ! -x ./sts ]]; then
    echo "Expected an executable at ./sts" >&2
    exit 1
fi
if [[ ! -r ./sts-test.yaml ]]; then
    echo "Expected a readable configuration at ./sts-test.yaml" >&2
    exit 1
fi

timestamp=$(date -u +%Y%m%dT%H%M%SZ)
role=$(hostname -s 2>/dev/null || hostname)
output="kcp-monitor-${role}-${timestamp}"
mkdir -p "$output" || exit 1

samples="$output/samples.csv"
sockets="$output/sockets.log"
metadata="$output/metadata.txt"
events="$output/events.log"
sts_log="$output/sts.log"

echo "Starting: ./sts -loop -conf ./sts-test.yaml"
./sts -loop -conf ./sts-test.yaml > "$sts_log" 2>&1 &
pid=$!
if ! kill -0 "$pid" 2>/dev/null; then
    wait "$pid"
    exit_code=$?
    echo "STS failed to start (exit $exit_code); see $sts_log" >&2
    exit "$exit_code"
fi

clock_ticks=$(getconf CLK_TCK 2>/dev/null || echo 100)
page_size=$(getconf PAGESIZE 2>/dev/null || echo 4096)

read_cpu() {
    awk '/^cpu / { total=0; for (i=2; i<=NF; i++) total += $i; print total, $5+$6 }' /proc/stat
}

read_process() {
    awk -v pages="$page_size" '{
        line=$0
        sub(/^.*\) /, "", line)
        n=split(line, field, " ")
        if (n < 37) { print "0 0 0 0 0"; exit }
        print field[12]+field[13], field[18], field[22]*pages, field[1], field[37]
    }' "/proc/$pid/stat" 2>/dev/null || echo "0 0 0 0 0"
}

read_io() {
    awk '
        /^read_bytes:/ { read_bytes=$2 }
        /^write_bytes:/ { write_bytes=$2 }
        END { print read_bytes+0, write_bytes+0 }
    ' "/proc/$pid/io" 2>/dev/null || echo "0 0"
}

read_net() {
    awk '
        NR > 2 {
            sub(/^[[:space:]]+/, "")
            split($0, field, /[:[:space:]]+/)
            if (field[1] != "lo") {
                rx += field[2]; rx_drop += field[5]
                tx += field[10]; tx_drop += field[13]
            }
        }
        END { print rx+0, tx+0, rx_drop+0, tx_drop+0 }
    ' /proc/net/dev
}

read_disk() {
    awk '
        $3 ~ /^(sd[a-z]+|vd[a-z]+|xvd[a-z]+|nvme[0-9]+n[0-9]+|mmcblk[0-9]+|md[0-9]+)$/ {
            read_sectors += $6
            write_sectors += $10
            io_ms += $13
        }
        END { print read_sectors*512, write_sectors*512, io_ms+0 }
    ' /proc/diskstats
}

read_udp() {
    awk '
        $1 == "Udp:" && header == "" { for (i=2; i<=NF; i++) name[i]=$i; header=1; next }
        $1 == "Udp:" && header == 1 {
            for (i=2; i<=NF; i++) value[name[i]]=$i
            print value["InDatagrams"]+0, value["OutDatagrams"]+0,
                  value["InErrors"]+0, value["RcvbufErrors"]+0,
                  value["SndbufErrors"]+0, value["NoPorts"]+0
            exit
        }
    ' /proc/net/snmp
}

read_memory() {
    awk '
        /^MemAvailable:/ { available=$2*1024 }
        /^MemTotal:/ { total=$2*1024 }
        END { print total+0, available+0 }
    ' /proc/meminfo
}

snapshot_sockets() {
    {
        echo "=== $(date -u +%Y-%m-%dT%H:%M:%S.%NZ) ==="
        if command -v ss >/dev/null 2>&1; then
            ss -uanmp 2>&1 | awk -v port=":$port" -v process="pid=$pid," \
                'NR == 1 || index($0, port) || index($0, process)'
        else
            echo "ss unavailable"
        fi
    } >> "$sockets"
}

{
    echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "role=$role"
    echo "host=$(hostname -f 2>/dev/null || hostname)"
    echo "pid=$pid"
    echo "port=$port"
    echo "interval_seconds=$interval"
    echo "duration_seconds=$duration"
    echo "sts_log=$sts_log"
    echo "clock_ticks=$clock_ticks"
    echo "page_size=$page_size"
    echo "kernel=$(uname -srmo)"
    echo "command=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null)"
    echo "sts_version=$(/proc/$pid/exe --version 2>&1 | head -1)"
    echo
    echo "--- limits ---"
    ulimit -a
    echo
    echo "--- UDP sysctls ---"
    for setting in net.core.rmem_max net.core.wmem_max net.core.netdev_max_backlog; do
        sysctl "$setting" 2>/dev/null || true
    done
    echo
    echo "--- interfaces ---"
    if command -v ip >/dev/null 2>&1; then
        ip -s link 2>&1
    else
        cat /proc/net/dev
    fi
} > "$metadata"

echo "utc,epoch,elapsed_s,role,pid,proc_cpu_pct,proc_threads,proc_rss_bytes,proc_state,proc_processor,proc_io_read_Bps,proc_io_write_Bps,system_cpu_pct,mem_total_bytes,mem_available_bytes,disk_read_Bps,disk_write_Bps,disk_busy_pct,net_rx_Bps,net_tx_Bps,net_rx_drops_delta,net_tx_drops_delta,udp_in_dgrams_delta,udp_out_dgrams_delta,udp_in_errors_delta,udp_rcvbuf_errors_delta,udp_sndbuf_errors_delta,udp_noports_delta" > "$samples"

read -r previous_cpu_total previous_cpu_idle <<< "$(read_cpu)"
read -r previous_proc_ticks previous_threads previous_rss previous_state previous_processor <<< "$(read_process)"
read -r previous_io_read previous_io_write <<< "$(read_io)"
read -r previous_disk_read previous_disk_write previous_disk_io_ms <<< "$(read_disk)"
read -r previous_net_rx previous_net_tx previous_net_rx_drop previous_net_tx_drop <<< "$(read_net)"
read -r previous_udp_in previous_udp_out previous_udp_error previous_udp_rcvbuf previous_udp_sndbuf previous_udp_noports <<< "$(read_udp)"
start_epoch=$(date +%s)
last_epoch=$start_epoch
running=1

stop() {
    running=0
    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
    fi
}
trap stop INT TERM

echo "Collecting role=$role pid=$pid every ${interval}s into $output"
echo "Press Ctrl-C to stop."
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) collector started" >> "$events"
snapshot_sockets

while ((running)); do
    sleep "$interval" &
    wait $! 2>/dev/null || true
    ((running)) || break

    now_epoch=$(date +%s)
    elapsed=$((now_epoch - start_epoch))
    sample_seconds=$((now_epoch - last_epoch))
    ((sample_seconds > 0)) || sample_seconds=$interval

    if [[ ! -r "/proc/$pid/stat" ]]; then
        echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) process $pid exited" | tee -a "$events"
        break
    fi

    read -r cpu_total cpu_idle <<< "$(read_cpu)"
    read -r proc_ticks threads rss state processor <<< "$(read_process)"
    read -r io_read io_write <<< "$(read_io)"
    read -r disk_read disk_write disk_io_ms <<< "$(read_disk)"
    read -r net_rx net_tx net_rx_drop net_tx_drop <<< "$(read_net)"
    read -r udp_in udp_out udp_error udp_rcvbuf udp_sndbuf udp_noports <<< "$(read_udp)"
    read -r mem_total mem_available <<< "$(read_memory)"

    cpu_delta=$((cpu_total - previous_cpu_total))
    idle_delta=$((cpu_idle - previous_cpu_idle))
    proc_delta=$((proc_ticks - previous_proc_ticks))
    if ((cpu_delta > 0)); then
        system_cpu=$(awk -v total="$cpu_delta" -v idle="$idle_delta" 'BEGIN { printf "%.2f", 100*(total-idle)/total }')
    else
        system_cpu="0.00"
    fi
    proc_cpu=$(awk -v ticks="$proc_delta" -v hz="$clock_ticks" -v seconds="$sample_seconds" 'BEGIN { printf "%.2f", 100*ticks/hz/seconds }')

    io_read_rate=$(((io_read - previous_io_read) / sample_seconds))
    io_write_rate=$(((io_write - previous_io_write) / sample_seconds))
    disk_read_rate=$(((disk_read - previous_disk_read) / sample_seconds))
    disk_write_rate=$(((disk_write - previous_disk_write) / sample_seconds))
    disk_busy=$(awk -v busy="$((disk_io_ms - previous_disk_io_ms))" -v seconds="$sample_seconds" 'BEGIN { printf "%.2f", busy/10/seconds }')
    net_rx_rate=$(((net_rx - previous_net_rx) / sample_seconds))
    net_tx_rate=$(((net_tx - previous_net_tx) / sample_seconds))

    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)" "$now_epoch" "$elapsed" "$role" "$pid" \
        "$proc_cpu" "$threads" "$rss" "$state" "$processor" \
        "$io_read_rate" "$io_write_rate" "$system_cpu" "$mem_total" "$mem_available" \
        "$disk_read_rate" "$disk_write_rate" "$disk_busy" "$net_rx_rate" "$net_tx_rate" \
        "$((net_rx_drop - previous_net_rx_drop))" \
        "$((net_tx_drop - previous_net_tx_drop))" "$((udp_in - previous_udp_in))" \
        "$((udp_out - previous_udp_out))" "$((udp_error - previous_udp_error))" \
        "$((udp_rcvbuf - previous_udp_rcvbuf))" "$((udp_sndbuf - previous_udp_sndbuf))" \
        "$((udp_noports - previous_udp_noports))" >> "$samples"

    previous_cpu_total=$cpu_total
    previous_cpu_idle=$cpu_idle
    previous_proc_ticks=$proc_ticks
    previous_io_read=$io_read
    previous_io_write=$io_write
    previous_disk_read=$disk_read
    previous_disk_write=$disk_write
    previous_disk_io_ms=$disk_io_ms
    previous_net_rx=$net_rx
    previous_net_tx=$net_tx
    previous_net_rx_drop=$net_rx_drop
    previous_net_tx_drop=$net_tx_drop
    previous_udp_in=$udp_in
    previous_udp_out=$udp_out
    previous_udp_error=$udp_error
    previous_udp_rcvbuf=$udp_rcvbuf
    previous_udp_sndbuf=$udp_sndbuf
    previous_udp_noports=$udp_noports
    last_epoch=$now_epoch

    snapshot_sockets
    if ((duration > 0 && elapsed >= duration)); then
        break
    fi
done

if kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null || true
fi
wait "$pid" 2>/dev/null || true
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) collector stopped" >> "$events"
snapshot_sockets
echo "Collection complete: $output"