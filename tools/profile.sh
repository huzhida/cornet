#!/usr/bin/env bash
#
# profile.sh — 通用性能分析工具
#
# 包装任意命令，自动完成：
#   1. 环境预检（perf 缺失时自动尝试安装，采样权限校验）
#   2. perf stat   —— 硬件/调度指标（IPC、cache miss、上下文切换等）
#   3. perf record —— 采样调用栈，解析热点 Top-N
#   4. 问题诊断    —— 基于指标与热点符号的启发式分析，输出 Markdown 报告
#
# 用法:
#   tools/profile.sh [选项] -- <命令> [参数...]
#   tools/profile.sh --pid <PID> --duration 10
#
# 常用示例:
#   tools/profile.sh --flame -- ./cmake-build-profile/bench
#   tools/profile.sh --duration 30 -- ./cmake-build-release/cornet-example
#   PERF=/path/to/perf tools/profile.sh -- ./a.out
#
set -euo pipefail
export LC_ALL=C

# ─────────────────────────── 阈值（启发式诊断，可调） ───────────────────────────
TH_IPC_WARN=1.0            # IPC 低于此值 -> 流水线停顿
TH_IPC_CRIT=0.5
TH_CACHE_MISS_WARN=10      # cache-miss / cache-references %
TH_CACHE_MISS_CRIT=20
TH_BRANCH_MISS_WARN=5      # branch-miss / branch-instructions %
TH_CTX_WARN=20000          # context-switches / 每秒(每核任务时间)
TH_CTX_CRIT=100000
TH_FAULTS_WARN=50000       # page-faults / 每秒
TH_MIGRATE_WARN=1000       # cpu-migrations / 每秒
TH_KERNEL_WARN=25          # 内核态采样占比 %
TH_LOCK_WARN=10            # 锁相关符号采样占比 %
TH_IO_WAIT_INFO=30         # IO 等待类符号占比 %
TH_TOP_SYM_WARN=40         # 单一符号占比 %

FREQ=99
CALL_GRAPH=auto            # auto|fp|dwarf|lbr
DURATION=0
ATTACH_PID=0
DO_STAT=1
DO_RECORD=1
DO_FLAME=0
DO_STRACE=0
ALLOW_INSTALL=1
ASSUME_YES=0
OUTPUT_DIR=""

# ──────────────────────────────── 输出着色 ────────────────────────────────
if [[ -t 1 ]]; then
    C_RED=$(tput setaf 1) C_YEL=$(tput setaf 3) C_GRN=$(tput setaf 2)
    C_BLU=$(tput setaf 4) C_BLD=$(tput bold) C_RST=$(tput sgr0)
else
    C_RED= C_YEL= C_GRN= C_BLU= C_BLD= C_RST=
fi
info()  { printf '%s[*]%s %s\n' "$C_BLU" "$C_RST" "$*"; }
ok()    { printf '%s[+]%s %s\n' "$C_GRN" "$C_RST" "$*"; }
warn()  { printf '%s[!]%s %s\n' "$C_YEL" "$C_RST" "$*" >&2; }
die()   { printf '%s[x]%s %s\n' "$C_RED" "$C_RST" "$*" >&2; exit 1; }

usage() {
    awk 'NR==1{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "$0"
    cat <<'EOF'
选项:
  -o, --output DIR     报告输出目录 (默认 profile_results/<时间戳>-<命令名>)
  -d, --duration SEC   只采样前 N 秒 (适合长驻进程; 结束后自动终止被测命令)
      --pid PID        attach 到已有进程 (需配合 --duration)
  -f, --freq HZ        采样频率 (默认 99)
      --call-graph M   调用栈模式 fp|dwarf|lbr (默认 auto, 自动降级)
      --stat-only      仅运行 perf stat
      --record-only    仅运行 perf record
      --flame          生成火焰图 SVG (FlameGraph 缺失时可自动拉取)
      --syscalls       附加 strace -c 系统调用统计 (会显著拖慢目标)
      --no-install     禁止自动安装依赖
  -y, --yes            全部确认项自动回答 yes
  -h, --help           显示本帮助

环境变量:
  PERF                 指定 perf 二进制路径
  FLAMEGRAPH_DIR       指定 FlameGraph 脚本目录
EOF
    exit 0
}

# ──────────────────────────────── 参数解析 ────────────────────────────────
CMD=()
while (( $# > 0 )); do
    case "$1" in
        -o|--output)    OUTPUT_DIR="$2"; shift 2 ;;
        -d|--duration)  DURATION="$2"; shift 2 ;;
        --pid)          ATTACH_PID="$2"; shift 2 ;;
        -f|--freq)      FREQ="$2"; shift 2 ;;
        --call-graph)   CALL_GRAPH="$2"; shift 2 ;;
        --stat-only)    DO_RECORD=0; shift ;;
        --record-only)  DO_STAT=0; shift ;;
        --flame)        DO_FLAME=1; shift ;;
        --syscalls)     DO_STRACE=1; shift ;;
        --no-install)   ALLOW_INSTALL=0; shift ;;
        -y|--yes)       ASSUME_YES=1; shift ;;
        -h|--help)      usage ;;
        --)             shift; CMD=("$@"); break ;;
        *)              die "未知参数: $1 (-h 查看帮助)" ;;
    esac
done

if (( ATTACH_PID > 0 )); then
    (( DURATION > 0 )) || die "--pid 需要配合 --duration"
    kill -0 "$ATTACH_PID" 2>/dev/null || die "进程 $ATTACH_PID 不存在"
    TARGET_NAME=$(ps -p "$ATTACH_PID" -o comm= 2>/dev/null || echo "pid$ATTACH_PID")
elif (( ${#CMD[@]} > 0 )); then
    TARGET_NAME=$(basename "${CMD[0]}")
else
    usage
fi

[[ -n "$OUTPUT_DIR" ]] || OUTPUT_DIR="profile_results/$(date +%Y%m%d-%H%M%S)-${TARGET_NAME}"
mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR=$(cd "$OUTPUT_DIR" && pwd)
PERF_DATA="$OUTPUT_DIR/perf.data"
STAT_CSV="$OUTPUT_DIR/stat.csv"
REPORT_TXT="$OUTPUT_DIR/report.txt"
REPORT_MD="$OUTPUT_DIR/report.md"
STRACE_TXT="$OUTPUT_DIR/strace.txt"

# ─────────────────────────────── 依赖: perf ───────────────────────────────
PERF_BIN=""

perf_works() { "$1" version >/dev/null 2>&1; }

detect_perf() {
    local c
    if [[ -n "${PERF:-}" ]]; then
        perf_works "$PERF" && { PERF_BIN="$PERF"; return 0; }
        warn "PERF=$PERF 不可用，继续探测"
    fi
    # Ubuntu linux-tools-common 的 wrapper 可能因内核版本不匹配而报错，需执行真正二进制
    for c in "$(command -v perf 2>/dev/null || true)" /usr/lib/linux-tools/*/perf /usr/lib/linux-kbuild/*/perf; do
        [[ -x "$c" ]] && perf_works "$c" && { PERF_BIN="$c"; return 0; }
    done
    return 1
}

install_perf() {
    (( ALLOW_INSTALL )) || die "未找到可用的 perf，且 --no-install 生效。请手动安装后重试"
    local SUDO=""
    (( EUID != 0 )) && command -v sudo >/dev/null && SUDO="sudo"
    info "未检测到 perf，尝试自动安装..."
    if command -v apt-get >/dev/null; then
        $SUDO apt-get update -qq || true
        $SUDO apt-get install -y "linux-tools-$(uname -r)" 2>/dev/null \
            || $SUDO apt-get install -y linux-tools-generic || die "apt 安装 perf 失败"
    elif command -v dnf >/dev/null; then
        $SUDO dnf install -y perf || die "dnf 安装 perf 失败"
    elif command -v yum >/dev/null; then
        $SUDO yum install -y perf || die "yum 安装 perf 失败"
    elif command -v pacman >/dev/null; then
        $SUDO pacman -S --noconfirm perf || die "pacman 安装 perf 失败"
    elif command -v zypper >/dev/null; then
        $SUDO zypper install -y perf || die "zypper 安装 perf 失败"
    else
        die "无法识别的包管理器，请手动安装 perf"
    fi
    detect_perf || die "安装后仍找不到可用的 perf 二进制"
}

check_paranoid() {
    local p
    p=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 99)
    info "kernel.perf_event_paranoid = $p"
    if (( p > 2 )); then
        warn "采样权限不足 (需要 <= 2)。"
        if (( EUID == 0 )); then
            if (( ASSUME_YES )) || { [[ -t 0 ]] && read -r -p "自动设置 kernel.perf_event_paranoid=2 ? [y/N] " a && [[ "$a" =~ ^[yY] ]]; }; then
                sysctl -w kernel.perf_event_paranoid=2 && ok "已调整"
                return
            fi
        fi
        die "请执行: sudo sysctl kernel.perf_event_paranoid=2 后重试"
    fi
}

# ───────────────────────────── 依赖: FlameGraph ─────────────────────────────
FLAME_DIR=""
find_flamegraph() {
    local d
    for d in "${FLAMEGRAPH_DIR:-}" "$(dirname "$0")/../third_party/FlameGraph" "$HOME/.cache/FlameGraph" /usr/share/FlameGraph; do
        [[ -n "$d" && -x "$d/stackcollapse-perf.pl" && -x "$d/flamegraph.pl" ]] && { FLAME_DIR="$d"; return 0; }
    done
    return 1
}
setup_flamegraph() {
    (( DO_FLAME )) || return 0
    find_flamegraph && { ok "FlameGraph: $FLAME_DIR"; return 0; }
    if (( ALLOW_INSTALL )) && command -v git >/dev/null; then
        local dst="$HOME/.cache/FlameGraph"
        info "拉取 FlameGraph -> $dst"
        timeout 60 git clone --depth 1 https://github.com/brendangregg/FlameGraph "$dst" \
            && find_flamegraph && { ok "FlameGraph: $FLAME_DIR"; return 0; }
    fi
    warn "FlameGraph 不可用，跳过火焰图 (可设 FLAMEGRAPH_DIR 指定)"
    DO_FLAME=0
}

# ─────────────────────────── 采集: perf stat / record ───────────────────────────
STAT_EVENTS="task-clock,context-switches,cpu-migrations,page-faults,cycles,instructions,branch-instructions,branch-misses,cache-references,cache-misses"
STAT_OK=0

probe_stat_events() {
    if "$PERF_BIN" stat -e "$STAT_EVENTS" -o /dev/null -- true >/dev/null 2>&1; then
        STAT_OK=1
    else
        warn "部分 perf event 不可用 (虚拟机/容器常见)，stat 阶段使用默认事件集"
    fi
}

TARGET_PID=""

run_collect() {
    local mode="$1"   # stat|record|strace
    local log="$2"
    shift 2
    local -a tool_cmd=("$@")

    if (( ATTACH_PID > 0 )); then
        # attach 模式: 目标可能在阶段间退出，先探活 (返回码 2 = 已退出)
        if ! kill -0 "$ATTACH_PID" 2>/dev/null; then
            warn "进程 $ATTACH_PID 已退出，跳过 $mode 阶段"
            return 2
        fi
        # 注意: 部分 perf 版本 attach 后退出码非 0 (数据已写入)，故不做硬失败
        case "$mode" in
            record) "${tool_cmd[@]}" --pid "$ATTACH_PID" -- sleep "$DURATION" &> "$log" || true ;;
            stat)   "${tool_cmd[@]}" --pid "$ATTACH_PID" -- sleep "$DURATION" &> "$log" || true ;;
            strace) timeout "$DURATION" strace -c -f -p "$ATTACH_PID" &> "$log" || true ;;
        esac
        return 0
    fi

    # 命令模式
    if (( DURATION > 0 )); then
        # 先启动命令，再按 PID 采样 DURATION 秒，最后终止命令
        "${CMD[@]}" &> "$OUTPUT_DIR/target.out" &
        TARGET_PID=$!
        sleep 0.3
        if ! kill -0 "$TARGET_PID" 2>/dev/null; then
            cat "$OUTPUT_DIR/target.out" >&2 || true
            die "被测命令启动后立即退出"
        fi
        case "$mode" in
            record) "${tool_cmd[@]}" --pid "$TARGET_PID" -- sleep "$DURATION" &> "$log" || true ;;
            stat)   "${tool_cmd[@]}" --pid "$TARGET_PID" -- sleep "$DURATION" &> "$log" || true ;;
            strace) timeout "$DURATION" strace -c -f -p "$TARGET_PID" &> "$log" || true ;;
        esac
    else
        # strace 不接受 "--" 分隔符，直接跟命令
        # 被测命令自身退出码不影响后续分析，统一容错
        if [[ "$mode" == strace ]]; then
            "${tool_cmd[@]}" "${CMD[@]}" &> "$log" || true
        else
            "${tool_cmd[@]}" -- "${CMD[@]}" &> "$log" || true
        fi
    fi
}

terminate_target() {
    if [[ -n "$TARGET_PID" ]] && kill -0 "$TARGET_PID" 2>/dev/null; then
        info "采样结束，终止被测命令 (pid=$TARGET_PID)"
        kill -INT "$TARGET_PID" 2>/dev/null || true
        local i
        for i in 1 2 3; do
            kill -0 "$TARGET_PID" 2>/dev/null || break
            sleep 1
        done
        kill -KILL "$TARGET_PID" 2>/dev/null || true
        wait "$TARGET_PID" 2>/dev/null || true
        TARGET_PID=""
    fi
}
trap terminate_target EXIT INT TERM

# ─────────────────────────────── 分析: stat ───────────────────────────────
# perf stat -x, 输出:  value,unit,event,runtime,percentage[,metric,unit]
stat_get() {  # stat_get <event-name> -> value|echo
    awk -F, -v ev="$1" '$3==ev && $1 !~ /^[<#]/ {print $1; exit}' "$STAT_CSV" 2>/dev/null || true
}

# ─────────────────────────────── 分析: report ───────────────────────────────
# report.txt 为 CSV: overhead,dso,symbol
# 兼容各版本 perf: 使用默认文本格式 "  41.01%  dso  [.] symbol(可含空格)" 再转 CSV
build_report_txt() {
    local raw
    if ! raw=$("$PERF_BIN" report --stdio --no-children -g none --percent-limit=0.3 \
            -F overhead,dso,symbol -i "$PERF_DATA" 2>>"$OUTPUT_DIR/record.log"); then
        warn "perf report 执行失败，跳过热点分析"
        return 0
    fi
    awk '$1 ~ /^[0-9.]+%$/ {
        ov=$1; sub(/%$/, "", ov); dso=$2
        sym=""; for(i=3;i<=NF;i++) sym = sym (i>3 ? " " : "") $i
        gsub(/,/, ";", dso)
        printf "%s,%s,%s\n", ov, dso, sym
    }' <<<"$raw" > "$REPORT_TXT"
    [[ -s "$REPORT_TXT" ]] || warn "report 无有效样本"
}

# 类别汇总: 输出 "类别 百分比" 列表
categorize() {
    awk -F, '
        $1 ~ /^[0-9.]+$/ {
            ov=$1; dso=$2; sym=$3; for(i=4;i<=NF;i++) sym=sym","$i
            cat="other"
            if (dso ~ /kernel\.kallsyms|\[kernel\]/ || sym ~ /^\[k\]/) cat="kernel"
            if (sym ~ /(futex|pthread_mutex|__lll_|_raw_spin|spin_lock|mutex_lock|read_lock|write_lock|sem_wait)/) cat="lock"
            if (sym ~ /(io_uring_enter|epoll_wait|ep_poll|__x64_sys_poll|do_sys_poll|tcp_recvmsg|lock_sock|sk_wait_data|io_submit)/) cat="io_wait"
            if (sym ~ /(schedule_timeout|__schedule$|finish_task_switch)/) cat="sched"
            if (sym ~ /(_int_malloc|_int_free|tc_malloc|tc_free|je_malloc|malloc$|free$|operator new|operator delete|__memalign)/) cat="alloc"
            if (sym ~ /(memcpy|memmove|memset)/) cat="memory_op"
            sum[cat]+=ov
        }
        END { for (c in sum) printf "%s %.2f\n", c, sum[c] }
    ' "$REPORT_TXT"
}

top_symbols() {  # top_symbols <n> <only-user:0/1>
    awk -F, -v n="$1" -v onlyuser="$2" '
        $1 ~ /^[0-9.]+$/ {
            sym=$3; for(i=4;i<=NF;i++) sym=sym","$i
            isk = ($2 ~ /kernel\.kallsyms|\[kernel\]/ || sym ~ /^\[k\]/)
            if (onlyuser && isk) next
            printf "%6.2f%%  %-40s %s\n", $1, $2, sym
            if (++c >= n) exit
        }
    ' "$REPORT_TXT"
}

# ─────────────────────────────── 诊断启发式 ───────────────────────────────
FINDINGS=()   # "level|标题|建议"
add_finding() { FINDINGS+=("$1|$2|$3"); }

fnum() { awk -v a="${1:-0}" -v b="$2" 'BEGIN{exit !(a+0 > b+0)}'; }  # a > b ?
lt()   { awk -v a="${1:-0}" -v b="$2" 'BEGIN{exit !(a+0 < b+0)}'; }

analyze_stat() {
    [[ -s "$STAT_CSV" ]] || return 0
    local cycles ins branch bmiss crefs cmiss ctx mig pf task
    cycles=$(stat_get cycles);            ins=$(stat_get instructions)
    branch=$(stat_get branch-instructions); bmiss=$(stat_get branch-misses)
    crefs=$(stat_get cache-references);   cmiss=$(stat_get cache-misses)
    ctx=$(stat_get context-switches);     mig=$(stat_get cpu-migrations)
    pf=$(stat_get page-faults);           task=$(stat_get task-clock)

    local ipc="" cmr="" bmr="" cps="" fps="" mps="" secs=""
    [[ -n "$cycles" && -n "$ins" && "$cycles" != "0" ]] && ipc=$(awk -v a="$ins" -v b="$cycles" 'BEGIN{printf "%.2f", a/b}')
    [[ -n "$crefs" && -n "$cmiss" && "$crefs" != "0" ]] && cmr=$(awk -v a="$cmiss" -v b="$crefs" 'BEGIN{printf "%.1f", 100*a/b}')
    # AMD/虚拟化环境 cache-misses 计数可能超过 references (口径含预取)，此时指标不可信
    if [[ -n "$cmr" ]] && fnum "$cmr" 100; then
        add_finding INFO "cache-miss 计数超过 references" "PMU 计数口径异常 (AMD/虚拟化常见)，本次忽略 cache miss 率指标"
        cmr=""
    fi
    [[ -n "$branch" && -n "$bmiss" && "$branch" != "0" ]] && bmr=$(awk -v a="$bmiss" -v b="$branch" 'BEGIN{printf "%.1f", 100*a/b}')
    if [[ -n "$task" && "$task" != "0" ]]; then
        secs=$(awk -v t="$task" 'BEGIN{printf "%.3f", t/1000}')
        [[ -n "$ctx" ]] && cps=$(awk -v a="$ctx" -v s="$secs" 'BEGIN{printf "%.0f", a/s}')
        [[ -n "$pf"  ]] && fps=$(awk -v a="$pf"  -v s="$secs" 'BEGIN{printf "%.0f", a/s}')
        [[ -n "$mig" ]] && mps=$(awk -v a="$mig" -v s="$secs" 'BEGIN{printf "%.0f", a/s}')
    fi

    echo "$ipc|$cmr|$bmr|$cps|$fps|$mps" > "$OUTPUT_DIR/.metrics"

    if [[ -z "$ipc$cmr$bmr$cps$fps$mps" ]]; then
        add_finding INFO "perf stat 无有效计数" "所有事件均为 <not counted> (容器/内核 PMU 限制)，本次仅热点采样可用"
        return 0
    fi

    [[ -n "$ipc" ]] && { lt "$ipc" "$TH_IPC_CRIT" && add_finding CRIT "IPC=$ipc 极低" "流水线严重停顿：疑似内存延迟/锁等待/分支预测失败，结合热点与 cache miss 排查" \
        || { lt "$ipc" "$TH_IPC_WARN" && add_finding WARN "IPC=$ipc 偏低" "有效指令吞吐不足，检查内存访问局部性与锁竞争"; }; }
    [[ -n "$cmr" ]] && { fnum "$cmr" "$TH_CACHE_MISS_CRIT" && add_finding CRIT "cache miss 率 ${cmr}%" "数据局部性差：考虑对象池、紧凑布局、减少指针跳转" \
        || { fnum "$cmr" "$TH_CACHE_MISS_WARN" && add_finding WARN "cache miss 率 ${cmr}%" "局部性一般，关注热路径上的数据结构布局"; }; }
    [[ -n "$bmr" ]] && fnum "$bmr" "$TH_BRANCH_MISS_WARN" && add_finding WARN "分支误预测率 ${bmr}%" "热分支不可预测：考虑 likely/unlikely、查表替代分支、拆分冷热路径"
    [[ -n "$cps" ]] && { fnum "$cps" "$TH_CTX_CRIT" && add_finding CRIT "上下文切换 ${cps}/s" "线程数远超核数或频繁挂起：减少线程、绑核、批量处理" \
        || { fnum "$cps" "$TH_CTX_WARN" && add_finding WARN "上下文切换 ${cps}/s" "切换较频繁，检查线程配置与唤醒风暴"; }; }
    [[ -n "$fps" ]] && fnum "$fps" "$TH_FAULTS_WARN" && add_finding WARN "page fault ${fps}/s" "内存分配/释放抖动：考虑预分配、内存池或调整分配器"
    [[ -n "$mps" ]] && fnum "$mps" "$TH_MIGRATE_WARN" && add_finding INFO "CPU 迁移 ${mps}/s" "线程在核间漂移，考虑 taskset/绑核提升 cache 亲和性"
    return 0
}

analyze_report() {
    [[ -s "$REPORT_TXT" ]] || return 0
    local cats kernel lock iow top1
    cats=$(categorize)
    kernel=$(awk '$1=="kernel"{print $2}' <<<"$cats");  kernel=${kernel:-0}
    lock=$(awk '$1=="lock"{print $2}' <<<"$cats");      lock=${lock:-0}
    iow=$(awk '$1=="io_wait"{print $2}' <<<"$cats");    iow=${iow:-0}
    top1=$(awk -F, '$1 ~ /^[0-9.]+$/ && $2 !~ /kernel/ {print $1; exit}' "$REPORT_TXT"); top1=${top1:-0}

    fnum "$kernel" "$TH_KERNEL_WARN" && add_finding WARN "内核态采样占比 ${kernel}%" "系统调用/内核路径开销大，建议追加 --syscalls 定位具体调用"
    fnum "$lock" "$TH_LOCK_WARN" && add_finding CRIT "锁相关符号占比 ${lock}%" "疑似锁竞争热点：减小临界区、无锁结构或分片"
    fnum "$iow" "$TH_IO_WAIT_INFO" && add_finding INFO "IO 等待符号占比 ${iow}%" "大量时间挂在 IO 等待属正常 (IO 密集)，优化方向是减少等待或提高并发"
    fnum "$top1" "$TH_TOP_SYM_WARN" && add_finding WARN "单一符号占比 ${top1}%" "存在绝对主导的热点函数，优先优化它收益最大"
    echo "$cats" > "$OUTPUT_DIR/.categories"
}

# ─────────────────────────────── 报告输出 ───────────────────────────────
emit_report() {
    local metrics="${1:-}"
    local ipc="" cmr="" bmr="" cps="" fps="" mps=""
    if [[ -n "$metrics" ]]; then
        IFS='|' read -r ipc cmr bmr cps fps mps <<<"$metrics"
    fi
    {
        echo "# 性能分析报告: $TARGET_NAME"
        echo
        echo "- 时间: $(date '+%F %T')"
        echo "- 主机: $(hostname) / $(uname -sr)"
        echo "- 命令: ${CMD[*]:-attach pid $ATTACH_PID}"
        (( DURATION > 0 )) && echo "- 采样时长: ${DURATION}s" || true
        echo "- perf: $PERF_BIN"
        echo
        if [[ -s "$STAT_CSV" ]]; then
            echo "## 关键指标 (perf stat)"
            echo
            echo "| 指标 | 值 |"
            echo "|---|---|"
            [[ -n "$ipc"  ]] && echo "| IPC | $ipc |"
            [[ -n "$cmr"  ]] && echo "| cache miss 率 | ${cmr}% |"
            [[ -n "$bmr"  ]] && echo "| 分支误预测率 | ${bmr}% |"
            [[ -n "$cps"  ]] && echo "| 上下文切换 | ${cps}/s |"
            [[ -n "$fps"  ]] && echo "| page faults | ${fps}/s |"
            [[ -n "$mps"  ]] && echo "| CPU 迁移 | ${mps}/s |"
            echo
        fi
        if [[ -s "$REPORT_TXT" ]]; then
            echo "## 热点 Top 15 (用户态)"
            echo
            echo '```'
            top_symbols 15 1
            echo '```'
            echo
            echo "## 类别汇总"
            echo
            echo '```'
            cat "$OUTPUT_DIR/.categories" 2>/dev/null | sort -k2 -rn
            echo '```'
            echo
        fi
        if ((${#FINDINGS[@]} > 0)); then
            echo "## 问题诊断"
            echo
            for f in "${FINDINGS[@]}"; do
                IFS='|' read -r lv title sugg <<<"$f"
                echo "- **[$lv]** $title"
                echo "  - 建议: $sugg"
            done
            echo
        fi
        echo "## 产物"
        echo
        for f in "$PERF_DATA" "$STAT_CSV" "$REPORT_TXT" "$STRACE_TXT" "$OUTPUT_DIR/flame.svg" "$OUTPUT_DIR/target.out"; do
            [[ -s "$f" ]] && echo "- \`$f\`" || true
        done
    } > "$REPORT_MD"
    return 0
}

print_summary() {
    echo
    echo "${C_BLD}════════ 采样指标 ════════${C_RST}"
    if [[ -f "$OUTPUT_DIR/.metrics" ]]; then
        local m; m=$(cat "$OUTPUT_DIR/.metrics")
        local ipc cmr bmr cps fps mps
        IFS='|' read -r ipc cmr bmr cps fps mps <<<"$m"
        if [[ -z "$ipc$cmr$bmr$cps$fps$mps" ]]; then
            echo "  (无有效计数，见诊断说明)"
            ipc=""; cmr=""; bmr=""; cps=""; fps=""; mps=""
        fi
        [[ -n "$ipc" ]] && printf '  IPC: %s\n' "$ipc"
        [[ -n "$cmr" ]] && printf '  cache miss: %s%%\n' "$cmr"
        [[ -n "$bmr" ]] && printf '  branch miss: %s%%\n' "$bmr"
        [[ -n "$cps" ]] && printf '  ctx-switch: %s/s\n' "$cps"
        [[ -n "$fps" ]] && printf '  page-faults: %s/s\n' "$fps"
    else
        echo "  (stat 阶段未执行或无数据)"
    fi
    if [[ -s "$REPORT_TXT" ]]; then
        echo
        echo "${C_BLD}════════ 热点 Top 10 ════════${C_RST}"
        top_symbols 10 1 | sed 's/^/  /'
    fi
    echo
    echo "${C_BLD}════════ 问题诊断 ════════${C_RST}"
    if ((${#FINDINGS[@]} == 0)); then
        echo "  ${C_GRN}未发现明显问题${C_RST}"
    else
        local f lv title sugg color
        for f in "${FINDINGS[@]}"; do
            IFS='|' read -r lv title sugg <<<"$f"
            case "$lv" in
                CRIT) color="$C_RED" ;; WARN) color="$C_YEL" ;; *) color="$C_BLU" ;;
            esac
            printf '  %s[%-4s]%s %s\n      -> %s\n' "$color" "$lv" "$C_RST" "$title" "$sugg"
        done
    fi
    echo
    ok "完整报告: $REPORT_MD"
    return 0
}

# ═══════════════════════════════ 主流程 ═══════════════════════════════
detect_perf || install_perf
ok "perf: $PERF_BIN ($("$PERF_BIN" version 2>/dev/null | head -1))"
check_paranoid
setup_flamegraph
probe_stat_events

if (( DO_STRACE )); then
    command -v strace >/dev/null || die "--syscalls 需要 strace"
fi

# 调用栈模式探测
if (( DO_RECORD )); then
    if [[ "$CALL_GRAPH" == auto ]]; then
        if "$PERF_BIN" record --call-graph fp -o /dev/null -- true >/dev/null 2>&1; then
            CALL_GRAPH=fp
        else
            CALL_GRAPH=dwarf
            warn "fp 栈回溯不可用，降级 dwarf (建议用 -fno-omit-frame-pointer 重新编译目标)"
        fi
    fi
    info "调用栈模式: $CALL_GRAPH, 频率: ${FREQ}Hz"
fi

# strace (独立、先跑或并行太重，顺序执行在最前，避免污染采样数据)
if (( DO_STRACE )); then
    info "阶段 0: strace -c 系统调用统计..."
    rc=0; run_collect strace "$STRACE_TXT" strace -c -f || rc=$?
    terminate_target
    (( rc == 0 )) && ok "strace 摘要已保存" || true
fi

if (( DO_STAT )); then
    info "阶段 1: perf stat 指标采集..."
    rc=0
    if (( STAT_OK )); then
        run_collect stat "$STAT_CSV.log" "$PERF_BIN" stat -x, -o "$STAT_CSV" -e "$STAT_EVENTS" || rc=$?
    else
        run_collect stat "$STAT_CSV.log" "$PERF_BIN" stat -x, -o "$STAT_CSV" || rc=$?
    fi
    terminate_target
    (( rc == 2 )) && DO_STAT=0 || ok "stat 完成"
fi

if (( DO_RECORD )); then
    info "阶段 2: perf record 采样..."
    rc=0
    run_collect record "$OUTPUT_DIR/record.log" \
        "$PERF_BIN" record -g --call-graph "$CALL_GRAPH" -F "$FREQ" -o "$PERF_DATA" || rc=$?
    terminate_target
    if (( rc == 2 )); then
        DO_RECORD=0
    elif [[ ! -s "$PERF_DATA" ]]; then
        die "perf.data 为空，采样失败 (日志: $OUTPUT_DIR/record.log)"
    else
        ok "采样完成: $(du -h "$PERF_DATA" | cut -f1)"
    fi
fi

if (( DO_FLAME )); then
    info "生成火焰图..."
    "$PERF_BIN" script -i "$PERF_DATA" 2>/dev/null \
        | "$FLAME_DIR/stackcollapse-perf.pl" \
        | "$FLAME_DIR/flamegraph.pl" > "$OUTPUT_DIR/flame.svg" \
        && ok "火焰图: $OUTPUT_DIR/flame.svg" \
        || warn "火焰图生成失败"
fi

# 分析
METRICS=""
if (( DO_STAT )) && [[ -s "$STAT_CSV" ]]; then
    analyze_stat
    METRICS=$(cat "$OUTPUT_DIR/.metrics" 2>/dev/null || true)
fi
if (( DO_RECORD )) && [[ -s "$PERF_DATA" ]]; then
    info "解析热点..."
    build_report_txt
    analyze_report
fi

emit_report "$METRICS"
print_summary
