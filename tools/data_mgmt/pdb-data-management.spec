Name: pdb-data-management
Summary: PalominoDB tools for data management
Version: 0.02
Vendor: PalominoDB
Release: 1
License: Private
Group: Application/System
Source: http://bastion.palominodb.com/releases/SRC/pdb-data-management-%{version}.tar.gz
URL: http://blog.palominodb.com
Requires: maatkit >= 5000
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%description
A growing collection of tools for managing growing datasets in MySQL.
Several of the tools make use of the highly robust and popular maatkit <http://maatkit.org> toolset.

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

%prep
%setup -q

%build

cd tools/data_mgmt
make

%install

%{__rm} -rf %{buildroot}
%{__mkdir} -p %{buildroot}
%{__install} -D -m 0755 tools/data_mgmt/bin/pdb-parted %{buildroot}/%{_bindir}/pdb-parted
%{__install} -D -m 0755 tools/data_mgmt/bin/pdb-archiver %{buildroot}/%{_bindir}/pdb-archiver

%clean

%{__rm} -rf %{buildroot}

%files
%defattr(0755,root,root)
%{_bindir}/pdb-parted
%{_bindir}/pdb-archiver

%files parted
%defattr(0755,root,root)
%{_bindir}/pdb-parted

%files archiver
%defattr(0755,root,root)
%{_bindir}/pdb-archiver
