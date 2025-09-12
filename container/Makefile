CONTAINER_NAME := zju-os-tool
IMAGE_NAME := git.zju.edu.cn:5050/os/tool
BUILD_ROOT_PATH := /zju-os/buildroot

# run on host
all:
	docker compose create
	docker compose start
	docker compose exec -it $(CONTAINER_NAME) /usr/bin/fish
	docker compose stop

clean:
	docker compose down -v --remove-orphans

build-image:
	docker build -t $(IMAGE_NAME) . # --no-cache

push-image:
	docker login git.zju.edu.cn:5050
	docker push git.zju.edu.cn:5050/os/tool

# inside docker
build-rootfs:
	make -C $(BUILD_ROOT_PATH) ARCH=riscv64 CROSS_COMPILE=riscv64-linux-gnu- qemu_riscv64_virt_defconfig
	make -C $(BUILD_ROOT_PATH) ARCH=riscv64 CROSS_COMPILE=riscv64-linux-gnu- -j$(nproc)
	cp $(BUILD_ROOT_PATH)/output/images/rootfs.ext2 /zju-os/tool/container
