Name:		mpifileutils
Version:	0.8
Release:	1%{?dist}
Summary:	File utilities designed for scalability and performance.

Group:		System Environment/Libraries
License:	Copyright and BSD License
URL:		https://hpc.github.io/mpifileutils
Source:		%{name}-%{version}.tar.gz
BuildRoot:      %_topdir/BUILDROOT
Requires: libcircle, lwgrp, dtcmp, libarchive, openssl, openssl-devel

%description
File utilities designed for scalability and performance.

%prep
%setup -q

%build
#topdir=`pwd`
#installdir=$topdir/install

#export PATH="${topdir}/autotools/install/bin:$PATH"
export PKG_CONFIG_PATH="$PKG_CONFIG_PATH:${installdir}/lib/pkgconfig"
export CC=mpicc
# hack to get things to build after common library
export CFLAGS="-I${topdir}/src/common -DDCOPY_USE_XATTRS"
export LDFLAGS="-Wl,-rpath,-lcircle"

%configure \
  --bindir=%{_bindir} \
  --enable-lustre \
  --disable-silent-rules \
  --with-dtcmp=${installdir} && \
make %{?_smp_mflags}

%install
rm -rf %{buildroot}
make install DESTDIR=%{buildroot}


%files
%defattr(-,root,root,-)
%{_bindir}/*
%{_includedir}/*
%{_libdir}/*
%{_mandir}/*

%changelog

