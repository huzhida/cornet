# cornet

# 1. 下载最新的 liburing
git clone https://github.com/axboe/liburing.git
cd liburing

# 2. 编译安装
./configure --prefix=/usr/local
make -j$(nproc)
sudo make install