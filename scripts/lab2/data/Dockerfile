# FROM registry.cn-shenzhen.aliyuncs.com/cse-lab/cse-lab2-base

# # Set the working directory
# WORKDIR /home/bin/

# # Run the binary
# # CMD ["./data"]
# CMD [ "sleep","100000" ]


FROM registry.cn-shenzhen.aliyuncs.com/cse-lab/cse-lab2-base
USER root
# 1. 安装 sudo
RUN apt-get update && apt-get install -y sudo

# Set the working directory
WORKDIR /home/bin/

# 2. 拷贝 meta 文件
COPY ../../build/bin/data ./data

# 3. 修改文件所有者为 stu
# RUN chown stu:stu ./data

# 保持容器运行
# CMD ["./data"]
CMD [ "sleep","100000" ]
