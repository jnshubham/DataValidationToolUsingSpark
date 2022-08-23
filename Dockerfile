FROM rockylinux/rockylinux
LABEL maintainer="Shubham Jain <shubhamakachamp@gmail.com>"
RUN mkdir dataValidation
WORKDIR /dataValidation
COPY . /dataValidation/
RUN yum install -y python3 java-1.8.0-openjdk java-1.8.0-openjdk-devel tar git wget zip gcc zlib-devel
RUN ln -s /usr/bin/pip3 /usr/bin/pip
RUN pip install -r config/requirements.txt
RUN yum clean all
RUN rm -rf /var/cache/yum
EXPOSE 5000
ENTRYPOINT ["bash", "/dataValidation/bin/gunicorn.sh"]
