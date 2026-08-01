# 项目构建说明

本项目使用 CMake。当需要构建项目时，请**始终**使用以下命令：

```bash
# 配置项目（首次或 CMakeLists.txt 变更后需要执行）
cmake --preset release

# 构建项目
cmake --build --preset release

# 构建bench
cmake --build --preset release --target bench

# 构建unit
cmake --build --preset release --target unit