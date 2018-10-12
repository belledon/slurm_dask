bootstrap: docker
from: continuumio/miniconda:latest

%setup
 if [ -d ${SINGULARITY_ROOTFS}/sbin ];then
    rm -r ${SINGULARITY_ROOTFS}/sbin
 fi
 mkdir ${SINGULARITY_ROOTFS}/sbin

%files
 sbin/* /sbin/

%environment
 export PATH=$PATH:/sbin/
 export LC_ALL=en_US.UTF-8

%runscript
  conda "$@"

%post
 chmod +x /sbin/*
