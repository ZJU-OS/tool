CONTAINER_NAME := zju-os-tool
IMAGE_NAME := git.zju.edu.cn:5050/os/tool
IMAGE_TAG := $(shell uname -m)
# aarch64 is same as arm64
ifeq ($(IMAGE_TAG),aarch64)
	IMAGE_TAG := arm64
endif
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
	docker buildx build \
		-t $(IMAGE_NAME):$(IMAGE_TAG) . # --no-cache

push-image:
	docker login git.zju.edu.cn:5050
	docker push git.zju.edu.cn:5050/os/tool:$(IMAGE_TAG)

push-multi:
	docker pull $(IMAGE_NAME):arm64
	docker pull $(IMAGE_NAME):x86_64
	docker manifest create $(IMAGE_NAME):latest \
		--amend $(IMAGE_NAME):arm64 \
		--amend $(IMAGE_NAME):x86_64
	docker manifest push $(IMAGE_NAME):latest

# https://www.docker.com/blog/multi-arch-build-and-images-the-simple-way/
# I can't make these methods work on my laptop
# build-push-multi:
# 	docker buildx build \
# 		--platform linux/amd64,linux/arm64 \
# 		--push \
# 		-t $(IMAGE_NAME) .
# build-push-multi:
# 	docker build \
# 		--platform linux/amd64 \
# 		-t $(IMAGE_NAME):latest .
# 	docker push $(IMAGE_NAME):latest
# 	docker build \
# 		--platform linux/arm64 \
# 		-t $(IMAGE_NAME):manifest-arm64 .
# 	docker push $(IMAGE_NAME):manifest-arm64
# 	docker manifest create $(IMAGE_NAME):latest \
# 		$(IMAGE_NAME):manifest-arm64 \
# 		$(IMAGE_NAME):latest
# 	docker manifest push $(IMAGE_NAME):latest

build-fat32:
	@if [ "$$(id -u)" -ne 0 ]; then \
		echo "Please use sudo to run this make target!"; \
		exit 1; \
	fi
	@if ! command -v parted >/dev/null 2>&1; then \
		echo "parted not found"; \
		exit 1; \
	fi
	@if ! command -v losetup >/dev/null 2>&1; then \
		echo "losetup not found"; \
		exit 1; \
	fi
	@if ! command -v mkfs.vfat >/dev/null 2>&1; then \
		echo "mkfs.vfat not found"; \
		echo "Run apt install dosfstools on Debian/Ubuntu"; \
		exit 1; \
	fi

	dd if=/dev/zero of=./fat32.img bs=1M count=64
	parted -s ./fat32.img -- \
		mklabel msdos\
		mkpart primary fat32 1MiB 100%
	LOOP_DEVICE=$$(losetup -f --show -P ./fat32.img); \
		mkfs.vfat $${LOOP_DEVICE}p1; \
		mkdir -p /mnt/fat32; \
		mount $${LOOP_DEVICE}p1 /mnt/fat32; \
		wget -O /mnt/fat32/email.txt https://ramix.org/linus.html; \
		umount /mnt/fat32; \
		losetup -d $${LOOP_DEVICE}; \
		rm -rf /mnt/fat32
	zip ./fat32.img.zip ./fat32.img
	rm -f ./fat32.img

# inside docker
build-rootfs:
	make -C $(BUILD_ROOT_PATH) ARCH=riscv64 CROSS_COMPILE=riscv64-linux-gnu- qemu_riscv64_virt_defconfig
	make -C $(BUILD_ROOT_PATH) ARCH=riscv64 CROSS_COMPILE=riscv64-linux-gnu- -j$(nproc)
	zip ./rootfs.ext2.zip $(BUILD_ROOT_PATH)/output/images/rootfs.ext2
