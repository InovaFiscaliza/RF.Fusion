FROM debian:wheezy

# Configura repositórios do archive
RUN echo "deb http://archive.debian.org/debian wheezy main contrib non-free" > /etc/apt/sources.list \
 && echo "Acquire::Check-Valid-Until false;" > /etc/apt/apt.conf

# Instala pacotes ignorando assinaturas expiradas
RUN apt-get update || true \
 && apt-get -o Acquire::AllowInsecureRepositories=true \
           -o Acquire::AllowDowngradeToInsecureRepositories=true \
           install -y --allow-unauthenticated \
           openssh-server cron procps net-tools \
 && mkdir -p /var/run/sshd \
 && ssh-keygen -A \
 && echo "root:changeme" | chpasswd

# Expõe a porta padrão do SSH
EXPOSE 22

# Inicia cron e sshd juntos
CMD service cron start && /usr/sbin/sshd -D
