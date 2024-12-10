FROM quay.io/pypa/manylinux2014_x86_64

# Install dependencies
RUN yum check-update || true
RUN yum install -y cmake unixODBC-devel openssl-devel

