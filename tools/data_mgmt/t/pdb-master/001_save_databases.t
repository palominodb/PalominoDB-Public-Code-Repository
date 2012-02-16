use strict;
use warnings FATAL => 'all';

use Test::More tests => 4;
use Test::MockObject;
use Digest::SHA;

BEGIN {
  my $mock = Test::MockObject->new();
  $mock->fake_module( 'File::Temp',
    'new' => sub {
      my ($class, $args) = @_;
      my $self = {};
      $self->{path} = "./pdb-mm-XXXXXXXXXX";
      return bless $self, $class;
    },
    'filename' => sub {
      my $self = shift;
      return $self->{path};
    },
    'unlink_on_destroy' => sub {
      my ($self, $v) = @_;
      $self->{'unlink'} = $v;
    },
    'DESTROY' => sub {
      my ($self) = @_;
      if($self->{'unlink'}) {
        unlink($self->{'path'});
      }
    }
  );
}

BEGIN {
  require_ok('src/pdb-master');
}

END {
  unlink('./pdb-mm-XXXXXXXXXX');
}

my $cfg = {
  'client' => {
    'port'   => 3600,
    'socket' => '/tmp/mysql_sandbox5145.sock'
  },
  'mysqld' => {
    'port'   => 3600,
    'socket' => '/tmp/mysql_sandbox5145.sock'
  }
};

my $r = pdb_master::save_databases(0, $cfg, 'root', 'msandbox', './', 'mysql');

is($r, './pdb-mm-XXXXXXXXXX', 'returned correct path');
ok(-f  './pdb-mm-XXXXXXXXXX', 'mysqldump exists');
diag("It's OK for the following test to fail - since size is an imperfect way to compare.");
is(-s  './pdb-mm-XXXXXXXXXX', 481037, 'sizes match');
