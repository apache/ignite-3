FROM centos:8


ENV TZ=Europe/Moscow
ARG RPM_WORK_DIR="/tmp"
ARG PACKAGE_VERSION


WORKDIR ${RPM_WORK_DIR}


# Install dependencies
RUN yum check-update || true && \
    yum install -y rpm-build


# Copy build required files
COPY apache-ignite.spec \
     ignite \
     build.sh \
     ./


# Run build
RUN bash build.sh ${PACKAGE_VERSION}

