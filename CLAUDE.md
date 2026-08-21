# 项目构建说明

本项目使用 CMake。当需要构建项目时，请**始终**使用以下命令：

```bash
# 配置项目（首次或 CMakeLists.txt 变更后需要执行）
cmake --preset release -DCORNET_ENABLE_BENCH=ON

# 构建项目
cmake --build --preset release

# 构建bench
cmake --build --preset release --target bench

# 构建unit
cmake --build --preset release --target unit
```

## 单元测试
```bash
# 配置项目
cmake --preset debug
# 构建
cmake --build --preset debug --target unit
```

## 性能分析

```bash
# 配置 profile 构建（RelWithDebInfo + 保留帧指针 + 导出符号，供采样分析用）
cmake --preset profile

# 构建
cmake --build --preset profile --target bench

# 一键分析：自动 perf stat + record + 热点 Top-N + 问题诊断，报告输出到 profile-results/
tools/profile.sh --flame -- ./cmake-build-profile/bench

# 长驻进程：只采样前 N 秒后自动终止
tools/profile.sh --duration 30 -- ./cmake-build-profile/your_server

# attach 已运行进程
tools/profile.sh --pid <PID> --duration 10
```