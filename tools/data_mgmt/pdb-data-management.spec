Name: pdb-data-management
Summary: PalominoDB tools for data management
Version: 0.08
Vendor: PalominoDB
Release: 1
License: BSD
Group: Application/System
Source: http://bastion.palominodb.com/releases/SRC/pdb-data-management-%{version}.tar.gz
URL: http://blog.palominodb.com
Requires: maatkit >= 5000
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%description
A growing collection of tools for managing growing datasets in MySQL.
Several of the tools make use of the highly robust and popular maatkit <http://maatkit.org> toolset.

%package packer
Summary: Just the table packing and rotating tool.
Group: Application/System

%description packer
A growing collection of tools for managing growing datasets in MySQL.
Several of the tools make use of the highly robust and popular maatkit <http://maatkit.org> toolset.

Just the table packing and rotating tool.

%package parted
Summary: Just the partition management tool.
Group: Application/System

%description parted
A growing collection of tools for managing growing datasets in MySQL.
Several of the tools make use of the highly robust and popular maatkit <http://maatkit.org> toolset.

Just the partition management tool.

%package archiver
Summary: Just the table archiving tool.
Group: Application/System
Requires: maatkit >= 5000

%description archiver
A growing collection of tools for managing growing datasets in MySQL.
Several of the tools make use of the highly robust and popular maatkit <http://maatkit.org> toolset.

Just the table archiving tool.

%package zrm-restore
Summary: Just the zrm-restore tool.
Group: Application/System

%description zrm-restore
A growing collection of tools for managing growing datasets in MySQL.
Several of the tools make use of the highly robust and popular maatkit <http://maatkit.org> toolset.

Just the zrm-restore tool.

%package sandbox-merge
Summary: Just the sandbox merging tool.
Group: Application/System

%description sandbox-merge
A growing collection of tools for managing growing datasets in MySQL.
Several of the tools make use of the highly robust and popular maatkit <http://maatkit.org> toolset.

Just the sandbox merging tool.

%package master
Summary: Just the cluster rebuild tool.
Group: Application/System

%description master
A growing collection of tools for managing growing datasets in MySQL.
Several of the tools make use of the highly robust and popular maatkit <http://maatkit.org> toolset.

Just the cluster rebuild tool.

%package munch
Summary: Just the data obfuscation tool.
Group: Application/System

%description munch
A growing collection of tools for managing growing datasets in MySQL.
Several of the tools make use of the highly robust and popular maatkit <http://maatkit.org> toolset.

Just the data obfuscation tool.

%prep
%setup -q

%build

cd tools/data_mgmt
make

%install

%{__rm} -rf %{buildroot}
%{__mkdir} -p %{buildroot}

for tool in pdb-{archiver,parted,packer,master,zrm-restore,sandbox-merge,munch}; do
  %{__install} -D -m 0755 tools/data_mgmt/bin/$tool %{buildroot}/%{_bindir}/$tool
done

%clean

%{__rm} -rf %{buildroot}

%files
%defattr(0755,root,root)
%{_bindir}/pdb-archiver
%{_bindir}/pdb-parted
%{_bindir}/pdb-packer
%{_bindir}/pdb-master
%{_bindir}/pdb-zrm-restore
%{_bindir}/pdb-sandbox-merge
%{_bindir}/pdb-munch

%files packer
%defattr(0755,root,root)
%{_bindir}/pdb-packer

%files parted
%defattr(0755,root,root)
%{_bindir}/pdb-parted

%files archiver
%defattr(0755,root,root)
%{_bindir}/pdb-archiver

%files master
%defattr(0755,root,root)
%{_bindir}/pdb-master

%files zrm-restore
%defattr(0755,root,root)
%{_bindir}/pdb-zrm-restore

%files sandbox-merge
%defattr(0755,root,root)
%{_bindir}/pdb-sandbox-merge

%files munch
%defattr(0755,root,root)
%{_bindir}/pdb-munch
