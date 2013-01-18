# SNMPPlugin.pm
# Copyright (C) 2009-2013 PalominoDB, Inc.
# 
# You may contact the maintainers at eng@palominodb.com.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

package SNMPPlugin;
use strict;
use warnings FATAL => 'all';
use Data::Dumper;
use IO::Handle;
use IO::Select;
use Time::HiRes;
use Carp;
$Data::Dumper::Indent = 0;

## Set SNMP_DEBUG to a true value in the environment
## To get debugging messages.
use constant DEBUG => $ENV{SNMP_DEBUG};

sub new($$$$$$) {
  my ($class, $root, $refresh_int, $inputfh, $outputfh, $logfh) = @_;
  my $self;
  if(ref($class)) {
    $self = $class;
    $class = ref($class);
  }
  else {
    $self = {};
  }

  $self->{_root} = $root;
  $self->{_refresh_interval} = int($refresh_int);
  $self->{_inputfh} = $inputfh;
  $self->{_outputfh} = $outputfh;
  if(DEBUG) {
    if(ref($logfh)) {
      $self->{_logfh} = $logfh;
    }
    else {
      open my $logf, ">>$logfh" or croak('Unable to open logfile:'. $!);
      $self->{_logfh} = $logf;
    }
  }
  else {
      open my $logf, ">>/dev/null" or croak('Unable to open logfile:'. $!);
      $self->{_logfh} = $logf;
  }
  $self->{Stats} = {};
  $self->{Oids}  = {};
  bless $self, $class;
  $self->initialize_tree();
  $self->{Next_List} = [oid_lex_sort(keys %{$self->{Oids}})];

  return $self;
}

sub DESTROY {
  my ($self) = @_;
  my $ofh = $self->{_outputfh};
  my $lfh = $self->{_logfh};
  $lfh->flush();
  $ofh->flush();
  close($lfh);
}

## oid_base_match and oid_lex_sort shamelessly lifted from Net-SNMP perl module
## http://search.cpan.org/~dtown/Net-SNMP-v6.0.0/lib/Net/SNMP.pm
sub oid_base_match($$) {
   my ($base, $oid) = @_;

   $base || return 0;
   $oid  || return 0;

   $base =~ s/^\.//o;
   $oid  =~ s/^\.//o;

   $base = pack('N*', split('\.', $base));
   $oid  = pack('N*', split('\.', $oid));

   (substr($oid, 0, length($base)) eq $base) ? 1 : 0;
}

sub oid_lex_sort(@) {
   return @_ unless (@_ > 1);

   map  { $_->[0] } 
   sort { $a->[1] cmp $b->[1] } 
   map  {
      my $oid = $_; 
      $oid =~ s/^\.//o;
      $oid =~ s/ /\.0/og;
      [$_, pack('N*', split('\.', $oid))]
   } @_;
}

sub next_oid($) {
  my ($self, $oid) = @_;
  my $Next_List = $self->{Next_List};
  my $i=0;
  while($Next_List->[$i] && oid_cmp($Next_List->[$i], $oid) != 1) { $i++; }
  if(!$Next_List->[$i]) {
    return undef;
  }
  return $Next_List->[$i];
}

sub oid_cmp($$) {
  my ($oid1, $oid2) = @_;
  $oid1 =~ s/^\.//o;
  $oid1 =~ s/ /\.0/og;
  $oid2 =~ s/^\.//o;
  $oid2 =~ s/ /\.0/og;
  return (pack('N*', split('\.', $oid1)) cmp pack('N*', split('\.', $oid2)));
}

sub logfh(;$) {
  my ($self, $fh) = @_;
  my $oldfh = $self->{_logfh};
  if(!$fh) { return $oldfh; }
  $self->{_logfh} = $fh;
  return $oldfh;
}

sub inputfh(;$) {
  my ($self, $fh) = @_;
  my $oldfh = $self->{_inputfh};
  if(!$fh) { return $oldfh; }
  $self->{_inputfh} = $fh;
  return $oldfh;
}

sub refresh_interval(;$) {
  my ($self, $i) = @_;
  my $oldi = $self->{_refresh_interval};
  if(!$i) { return $oldi; }
  $self->{_refresh_interval} = $i;
  return $oldi;
}

sub l(@) {
 my $self = shift; 
 my $fh = $self->{_logfh};
 my $prefix = sprintf("%.3f: ",time());
 print($fh $prefix,( map { (my $temp = $_) =~ s/\n/\n$prefix/g; $temp; }
           map { defined $_ ? $_ : 'undef' } @_), "\n");
}

sub add_oid($$$) {
  my ($self, $frag, $value) = @_;
  $$self{Oids}{"$$self{_root}$frag"} = $value;
}

###############################################################################
## TO BE IMPLEMENTED IN DECENDENT PACKAGES
## In general, the default implementation for everything but 'set' should
## be sufficient. If you override, you must make a call to write_value()
## or, the plugin will stall as will snmpd.
###############################################################################
sub getnext($$) {
  my ($self, $oid) = @_;
  my $next_oid = $self->next_oid($oid);
  if($next_oid) {
    $self->write_value($next_oid, @{$$self{Oids}{$next_oid}});
  }
  else {
    $self->write_value($oid, 'none');
  }
}

sub get($$) {
  my ($self, $oid) = @_;
  if(exists $$self{Oids}{$oid}) {
    $self->write_value($oid, @{$$self{Oids}{$oid}});
  }
  else {
    $self->write_value($oid, 'none');
  }
}

sub set($$$){
  my ($self, $oid, $type) = @_;
  $self->write_value($oid, 'not-writeable');
}

sub initialize_tree() {
  my $self = @_;
  $self->{Stats} = {};
}

sub update_statistics() {
  return 0;
}
###############################################################################
## TO BE IMPLEMENTED IN DECENDENT PACKAGES
###############################################################################

# This receives an OID on STDIN and makes it nice for us.
sub read_oid {
  my $self = shift;
  my $ifh = $self->{_inputfh};
  my $oid_str = <$ifh>;
  chomp($oid_str);
  DEBUG && $self->l('> ', $oid_str);
  return $oid_str;
}

# This does the grunt work for both cmd_get and cmd_getnext
# it takes care of returning the actual values to snmpd over STDOUT
sub write_value {
  my $self = shift;
  my $ofh = $self->{_outputfh};
  DEBUG && $self->l('write_value: ', Dumper(\@_));
  my ($oid, $type, $value) = @_;
  my $oid_str = $oid;
  if($type eq 'code') {
    my $res = &$value();
    DEBUG && $self->l('write_value code: ', Dumper($res));
    ($type, $value) = ($res->[0], $res->[1]);
  }
  if($type eq 'none') {
    DEBUG && $self->l('< NONE\n');
    print $ofh "NONE\n";
    return;
  }
  if(not defined($value)) {
    DEBUG && $self->l("< $type\\n");
    print $ofh $type, "\n";
    return;
  }
  elsif(ref($value)) {
    $value = $$value;
  }
  DEBUG && $self->l("< $oid_str\\n$type\\n$value\\n");
  print $ofh $oid_str, "\n", $type, "\n", $value, "\n";
  return;
}


sub run() {
  my $self = shift;
  my $s = IO::Select->new();
  my $ifh = $self->{_inputfh};
  my $ofh = $self->{_outputfh};
  my $lfh = $self->{_logfh};
  $s->add($ifh);

  $SIG{__DIE__} = sub { $self->l('DIE:', @_); $self->{_logfh}->flush(); };
  $SIG{__WARN__} = sub { $self->l('WARN:', @_); $self->{_logfh}->flush(); };


  while(1) {
    my @ready = $s->can_read($self->{_refresh_interval});
    if(!@ready) {
      DEBUG && $self->l('timeout: updating statistics.');
      $self->update_statistics();
    }
    else {
      $_ = <$ifh>;
      if(not defined($_)) {
        last; # Leave mainloop on EOF;
      }
      chomp;
      DEBUG && $self->l('> ', $_, '\n');
      if($_ eq 'PING') {
        print $ofh "PONG\n";
        $ofh->flush;
        next;
      }
      elsif($_ eq 'getnext') {
        my $oid = $self->read_oid();
        $self->getnext($oid);
      }
      elsif($_ eq 'get') {
        my $oid = $self->read_oid();
        $self->get($oid);
      }
      elsif($_ eq 'set') {
        my $oid = $self->read_oid();
        my $type = <$ifh>;
        chomp($type);
        $self->set($oid, $type);
      }
      else {
        croak('Unknown command: '. $_);
      }
      $lfh->flush();
      $ofh->flush();
    }
  } # while(1)
}

1;
