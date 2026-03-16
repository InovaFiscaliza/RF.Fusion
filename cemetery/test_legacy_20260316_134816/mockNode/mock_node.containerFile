FROM debian:wheezy

# Install SSH server
RUN apt-get update && apt-get install -y openssh-server \
 && mkdir /var/run/sshd

# Create SFTP user (instead of root)
RUN useradd -m -d /home/device -s /bin/bash device \
 && echo "device:devicepass" | chpasswd

# Ensure the user’s folder is owned properly
RUN mkdir -p /home/device/data && chown -R device:device /home/device

# Expose SFTP port
EXPOSE 22

# Run sshd in foreground
CMD ["/usr/sbin/sshd", "-D"]