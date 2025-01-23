# stage 1: download the prebuilt toolchain
FROM --platform=$BUILDPLATFORM ubuntu:noble AS downloader

ARG TARGETARCH

WORKDIR /tmp

# select the toolchain according to the target architecture
RUN apt-get update && apt-get install -y --no-install-recommends wget ca-certificates \
    && if [ "$TARGETARCH" = "arm64" ]; then \
        TOOLCHAIN_URL="https://github.com/ZJU-CP/tools/releases/download/v13.2.0/riscv32-unknown-elf-gcc-13.2.0-rv32gc-ilp32d-aarch64-linux-ubuntu-24.04.tar.gz"; \
    else \
        TOOLCHAIN_URL="https://github.com/ZJU-CP/tools/releases/download/v13.2.0/riscv32-unknown-elf-gcc-13.2.0-rv32gc-ilp32d-x86_64-linux-ubuntu-20.04.tar.gz"; \
    fi \
    && wget -q --show-progress --progress=bar:force:noscroll $TOOLCHAIN_URL \
    && tar -zxf *.tar.gz \
    && rm *.tar.gz

# stage 2: build the final image
FROM ubuntu:noble

ENV TZ=Asia/Shanghai \
    DEBIAN_FRONTEND=noninteractive

# use ZJU mirror to speed up apt-get
# RUN sed -i -e 's|http://archive.ubuntu.com/ubuntu/|http://mirrors.zju.edu.cn/ubuntu/|g' \
#         -e 's|http://security.ubuntu.com/ubuntu|http://mirrors.zju.edu.cn/ubuntu/|g' \
#         /etc/apt/sources.list.d/ubuntu.sources

# install necessary packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    wget \
    python3 \
    python3-pip \
    qemu-user \
    flex \
    bison \
    openjdk-17-jdk \
    python3-lark \
    python3-venv \
    python3-toml \
    python3-rich \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# copy the prebuilt toolchain
COPY --from=downloader /tmp/riscv32-unknown-elf-gcc-* /opt/riscv32-prebuilt

ENV PATH="/opt/riscv32-prebuilt/bin:$PATH"

CMD ["/bin/bash"]
