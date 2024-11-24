# FROM registry.cn-shenzhen.aliyuncs.com/cse-lab/cse-lab2-base

# # Set the working directory
# WORKDIR /home/stu

# # Run the binary
# # CMD ["./fs_client", "/tmp/mnt"]
# CMD [ "sleep","100000" ]


FROM registry.cn-shenzhen.aliyuncs.com/cse-lab/cse-lab2-base
USER root
# 1. 安装 sudo
RUN apt-get update && apt-get install -y sudo

# Set the working directory
WORKDIR /home/stu/

# 2. 拷贝 fs_client 文件
COPY ../../build/bin/fs_client ./fs_client

# 3. 修改文件所有者为 stu
# RUN chown stu:stu ./fs_client

# 保持容器运行
# CMD ["./fs_client", "/tmp/mnt"]
CMD [ "sleep","100000" ]
