FROM registry.suse.com/bci/bci-base:15.3

ARG ARCH=amd64

RUN zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/SLE_15/system:snappy.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/devel:tools:scm/SLE_15_SP3/devel:tools:scm.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/network:utilities/SLE_15_SP3/network:utilities.repo && \
    zypper --gpg-auto-import-keys ref

RUN zypper -n install cmake kmod curl nfs-client nfs4-acl-tools fuse git gcc \
    libibverbs librdmacm1 rdma-core-devel perl-Config-General libaio1 sg3_utils \
    iputils telnet iperf qemu-tools wget iproute2 xsltproc docbook-xsl-stylesheets e2fsprogs \
    autoconf libtool libunwind-devel gcc-c++ && \
    rm -rf /var/cache/zypp/*

# Install grpc_health_probe
RUN wget https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.2/grpc_health_probe-linux-${ARCH} -O /usr/local/bin/grpc_health_probe && \
    chmod +x /usr/local/bin/grpc_health_probe

# Build jemalloc
RUN if [ ${ARCH} == "amd64" ]; then \
        cd /usr/src && \
        git clone https://github.com/jemalloc/jemalloc.git && \
        cd jemalloc && \
        git checkout 54eaed1d8b56b1aa528be3bdd1877e59c56fa90c && \
        ./autogen.sh && \
        make && \
        make install; \
    fi

RUN ldconfig

# Build liblonghorn
RUN cd /usr/src && \
    git clone https://github.com/derekbit/liblonghorn.git && \
    cd liblonghorn && \
    git checkout 5878aa9f64de04953a7cbddfa473023f9623c136 && \
    if [ ${ARCH} == "amd64" ]; then \
        make EXTRA_LIBS=/usr/local/lib/libjemalloc.a; \
    else \
        make; \
    fi; \
    make install

# Build TGT
RUN cd /usr/src && \
    git clone https://github.com/derekbit/tgt.git && \
    cd tgt && \
    git checkout db72266aa0a736ad972234016398b5bf19752676 && \
    if [ ${ARCH} == "amd64" ]; then \
        make EXTRA_LIBS=-ljemalloc; \
    else \
        make; \
    fi; \
    make install

COPY bin/longhorn bin/longhorn-instance-manager /usr/local/bin/

COPY package/launch-simple-longhorn package/engine-manager package/launch-simple-file /usr/local/bin/

VOLUME /usr/local/bin

# Add Tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${ARCH} /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

CMD ["longhorn"]
