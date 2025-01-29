Name:		mpifileutils
Version:	0.12
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
%{cmake} ./ -DWITH_DTCMP_PREFIX=${installdir} -DWITH_LibCircle_PREFIX=${installdir} -DCMAKE_INSTALL_PREFIX=%{buildroot} -DENABLE_LUSTRE=ON -DENABLE_XATTRS=ON
%{cmake_build}

%install
rm -rf %{buildroot}
%{cmake_install}


%files
%defattr(-,root,root,-)
%{_bindir}/*
%{_includedir}/*
%{_libdir}/*
%{_mandir}/*

%changelog
