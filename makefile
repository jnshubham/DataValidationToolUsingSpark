.PHONY: initialize build run kill
app_name = datavalidation:v2
setup:
    @echo 'Updating the packages'
    @sudo yum update -y
    @sudo yum install -y yum-utils
    @echo 'Downloading Docker'
    @sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
    @echo 'Installing Docker'
    @sudo yum install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

initialize:
    @echo 'Starting Docker Service'
    @sudo systemctl start docker
build:
    @echo 'Building docker image'
    @sudo docker build -t $(app_name) .
run:
    @echo 'Running docker container for image $(app_name)'
    sudo docker run --detach -p 5000:5000 $(app_name)
kill:
    @echo 'Killing container'
    @sudo docker ps | grep $(app_name) | awk '{print $$1}' | xargs sudo docker stop
