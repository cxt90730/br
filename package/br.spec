%global debug_package %{nil}
%global __strip /bin/true

Name:           br
Version:        %{ver}
Release:        %{rel}%{?dist}

Summary:	br is backup && restore for tikv

Group:		SDS
License:    Apache-2.0
URL:		http://github.com/cxt90730/br
Source0:	%{name}-%{version}-%{rel}.tar.gz
BuildRoot:	%(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)
#BuildRequires:  

%description


%prep
%setup -q -n %{name}


%build
#The go build still use source code in GOPATH/src/legitlab/yig/
#keep git source tree clean, better ways to build?
#I do not know
make build_internal

%install
rm -rf %{buildroot}
install -D -m 755 bin/br %{buildroot}%{_bindir}/br
install -D -m 644 package/br.logrotate %{buildroot}/etc/logrotate.d/br.logrotate
install -D -m 644 package/br.service   %{buildroot}/usr/lib/systemd/system/br.service
install -D -m 644 package/run_backup.sh %{buildroot}%{_sysconfdir}/br/run_backup.sh
install -D -m 644 package/run_restore.sh %{buildroot}%{_sysconfdir}/br/run_restore.sh
install -d %{buildroot}/var/log/br/

#ceph confs ?

%post
systemctl enable br

%preun

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%config(noreplace) /etc/br/run_backup.sh
%config(noreplace) /etc/br/run_restore.sh
/usr/bin/br
/etc/logrotate.d/br.logrotate
%dir /var/log/br/
/usr/lib/systemd/system/br.service

%changelog
