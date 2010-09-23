
%{!?dist:%define dist site}

Name: mysqlctl
Summary: Better mysql init script
Version: 0.04
Vendor: PalominoDB
Release: 2
License: BSD
Group: Application/System
Source: http://bastion.palominodb.com/releases/SRC/mysqlctl-%{version}.tar.gz
URL: http://blog.palominodb.com
Requires: bash >= 3.2, mysql
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%description
Better init script for mysql that does
slave starting/stopping, log flushing
and has useful exit codes for automation tools.

%package init-%{dist}
Summary: The init.d link for mysqlctl
Group: Application/System
Requires: mysqlctl = %{version}

%description init-%{dist}
Better init script for mysql that does
slave starting/stopping, log flushing
and has useful exit codes for automation tools.

This package provides a symlink to mysqlctl
so it can be used as an init script.

%prep
%setup -q

%build

%install

%{__rm} -rf %{buildroot}
%{__mkdir} -p %{buildroot}
%{__install} -D -m 0755 mysqlctl %{buildroot}/%{_sbindir}/mysqlctl
%{__install} -D -m 0600 sample-myctl.cnf %{buildroot}/%{_sysconfdir}/myctl.cnf

%{__mkdir} -p %{buildroot}/%{_initrddir}
%{__mkdir} -p %{buildroot}/%{_datadir}/%{name}
%{__ln_s} %{_sbindir}/mysqlctl %{buildroot}/%{_initrddir}/%{dist}-mysql

%{__install} -m 0644 hook_scripts/* %{buildroot}/%{_datadir}/%{name}/

%clean

%{__rm} -rf %{buildroot}

%files
%defattr(0755,root,root)
%doc README
%{_sbindir}/mysqlctl
%attr(0600,root,root)
%config %{_sysconfdir}/myctl.cnf
%defattr(0644,root,root)
%{_datadir}/%{name}/

%files init-%{dist}
%{_initrddir}/%{dist}-mysql
