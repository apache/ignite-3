FROM rockylinux:8
# Install dependencies
RUN dnf update -y
RUN dnf -y install epel-release
RUN dnf update -y
RUN dnf install -y --setopt=max_parallel_downloads=10 --setopt=fastestmirror=True git gcc gcc-c++ clang make cmake unixODBC-devel openssl-devel java-11-openjdk-devel java-17-openjdk-devel rpm-build dpkg
RUN dnf clean all
RUN rm -rf /var/cache/dnf
