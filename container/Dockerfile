# https://docs.docker.com/reference/dockerfile/
FROM ghcr.io/astral-sh/uv:latest AS uv-distroless

FROM debian:testing

ENV DEBIAN_FRONTEND=noninteractive

RUN cat <<EOF > /etc/apt/sources.list.d/debian.sources
Types: deb deb-src
URIs: http://mirrors.zju.edu.cn/debian/
Suites: testing testing-updates testing-backports
Components: main contrib non-free non-free-firmware
Signed-By: /usr/share/keyrings/debian-archive-keyring.gpg

Types: deb deb-src
URIs: http://mirrors.zju.edu.cn/debian-security/
Suites: testing-security
Components: main contrib non-free non-free-firmware
Signed-By: /usr/share/keyrings/debian-archive-keyring.gpg
EOF

RUN <<EOF
apt-get update -y
mkdir -p /zju-os
EOF
WORKDIR /zju-os

# Locale
RUN <<EOF
apt-get install -y locales
sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen
locale-gen
EOF
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ENV LANGUAGE=en_US:en

# Kernel source code and build tools
# https://www.debian.org/doc/manuals/debian-kernel-handbook/ch-common-tasks.html
RUN <<EOF
apt-get build-dep -y linux
apt-get install -y linux-source build-essential fakeroot devscripts rsync git
tar xaf /usr/src/linux-source-*.tar.xz -C /zju-os
EOF

# Cross compile toolchain
RUN <<EOF
apt-get install -y \
    crossbuild-essential-riscv64 \
    gdb-multiarch \
    qemu-system-riscv
EOF

# Utils
RUN <<EOF
apt-get install -y fish vim tree tmux \
    clangd bear clang-format clang-tidy \
    wget curl btop
chsh -s /usr/bin/fish
EOF

# Rootfs
COPY *.zip .
RUN <<EOF
unzip fat32.img.zip
unzip rootfs.ext2.zip
rm *.zip
EOF
ADD https://mirrors.zju.edu.cn/debian/dists/unstable/main/installer-riscv64/current/images/netboot/debian-installer/riscv64/initrd.gz .

# Spike
# ADD https://github.com/riscv-software-src/riscv-isa-sim.git riscv-isa-sim
ADD https://fast-mirror.isrc.ac.cn/riscv-toolchains/git/riscv-software-src/riscv-isa-sim.git riscv-isa-sim
RUN <<EOF
apt-get install -y openocd \
    device-tree-compiler libboost-regex-dev libboost-system-dev
cd riscv-isa-sim
mkdir build
cd build
../configure
make -j$(nproc)
make install
cd ../..
rm -rf riscv-isa-sim
EOF

# uv
COPY --from=uv-distroless /uv /uvx /usr/local/bin

# source code
RUN <<EOF
apt-get source qemu-system-riscv
rm -f qemu_*.dsc qemu_*.tar.*
EOF

# Clean up
RUN <<EOF
apt-get autoremove
apt-get clean
EOF
