#!/usr/bin/env perl
use strict;
use warnings FATAL => 'all';

# ###########################################################################
# ProcessLog package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End ProcessLog package
# ###########################################################################

# ###########################################################################
# MysqlInstance package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End MysqlInstance package
# ###########################################################################

# ###########################################################################
# Which package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End Which package
# ###########################################################################

# ###########################################################################
# IniFile package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End IniFile package
# ###########################################################################

# ###########################################################################
# DSN package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End DSN package
# ###########################################################################

# ###########################################################################
# TableAge package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End TableAge package
# ###########################################################################

# ###########################################################################
# TablePacker package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End TablePacker package
# ###########################################################################

# ###########################################################################
# TableRotater package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End TableRotater package
# ###########################################################################

# ###########################################################################
# RObj package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End RObj package
# ###########################################################################

package pdb_packer;
use strict;
use warnings FATAL => 'all';

use DBI;
use Getopt::Long qw(:config no_ignore_case);
use Pod::Usage;
use DateTime;
use Data::Dumper;
use Sys::Hostname;
use DateTime;
$Data::Dumper::Indent = 0;

use ProcessLog;
use MysqlInstance;
use DSN;
use TableAge;
use TablePacker;
use TableRotater;
use RObj;

our $VERSION = 0.032;

use constant DEFAULT_LOG => "/dev/null";
use constant DEFAULT_DATE_FORMAT => "_%Y%m%d";

my $logfile  = DEFAULT_LOG;
my $age      = 0;

# These are 'our' so that testing can fiddle with them easily.
my $pretend  = 0;
my $pack     = 0;
my $rotate   = 0;
my $age_format = "_%Y%m%d";
my $rotate_format = '';
my $cur_date = DateTime->now( time_zone => 'local' )->truncate( to => 'day' );
my $force    = 0;

sub main {
  # Overwrite ARGV with parameters passed here
  # This means, you must save ARGV before calling this
  # if you want to have ARGV later.
  @ARGV = @_;
  my @DSNs = ();
  my $dsnp = DSNParser->default();
  my $pl;
  $dsnp->mand_key('h', 1);
  $dsnp->mand_key('D', 1);
  $dsnp->mand_key('sU', 1);
  $dsnp->add_key('r', { 'mandatory' => 0, 'desc' => 'Table name prefix' });
  $dsnp->add_key('rF', { 'mandatory' => 0, 'desc' => 'Remote my.cnf' });
  GetOptions(
    "help" => sub {
      pod2usage( -verbose => 1 );
    },
    "pretend|p" => \$pretend,
    "logfile=s" => \$logfile,
    "rotate" => \$rotate,
    "pack" => \$pack,
    "age=s" => \$age,
    "age-format=s" => \$age_format,
    "rotate-format=s" => \$rotate_format,
    "force" => \$force
  );

  unless(scalar @ARGV >= 1) {
    pod2usage(-message => 'Need at least one DSN to operate on', -verbose => 1);
  }

  if($age and $age =~ /(\d+)([wmyd])/i) {
    my %keys = ( w => 'weeks', m => 'months', y => 'years', d => 'days' );
    $age = DateTime::Duration->new( $keys{$2} => $1 );
    $age = $cur_date - $age;
  }
  elsif($age) {
    pod2usage(-message => 'Age: "' . $age . '" does not match format.',
      -verbose => 1);
  }

  $pl = ProcessLog->new($0, $logfile);
  $pl->i("pdb-packer v$VERSION build GIT_SCRIPT_VERSION");

  # Shift off the first DSN, parse it,
  # and then make some keys non-mandatory.
  # The remaining DSNs will fill in from this one.
  push(@DSNs, $dsnp->parse(shift(@ARGV)));
  $dsnp->mand_key('D', 0);
  $dsnp->mand_key('sU', 0);

  # Parse remaining DSNs and fill in any missing values.
  for(@ARGV) {
    push(@DSNs, $dsnp->parse($_));
    $DSNs[-1]->fill_in($DSNs[0]);
  }

  for(@DSNs) {
    if($age and $age_format ne 'createtime' and !$_->has('r')) {
      $pl->e('DSN:', $_->str(), 'is missing required key', 'r',
        'for --age-format');
      return 1;
    }
    unless($_->has('t') or $_->has('r')) {
      $pl->e('DSN:', $_->str(), 'is missing one of the required keys: t or r');
      return 1;
    }
    unless($_->get('r') =~ /\(.*?\)/) {
      $pl->e('DSN:', $_->str(), 'r key does not have a capture group.');
      return 1;
    }
    if($_->has('t') and $_->has('r')) {
      $pl->e('DSN:', $_->str(), 'has both t and r. You must use only one.');
      return 1;
    }
  }

  foreach my $d (@DSNs) {
    my $dbh = $d->get_dbh(1);
    my @tbls = @{get_tables($d)};
    $pl->m('Working Host:', $d->get('h'), ' Working DB:', $d->get('D'));
    $pl->d('tables:', join(',', @tbls) );  
    my $cfg  = MysqlInstance->from_dsn($d)->config();
    $pl->d('Host config:', Dumper($cfg));
    $pl->d('Host datadir:', $cfg->{'mysqld'}->{'datadir'});
    foreach my $t (@tbls) {
      # Set the table key in the DSN
      # Rather than making a new DSN for each table, we just
      # overwrite the key - saves space.
      $d->{'t'}->{'value'} = $t;
      my $r;
      if($age and table_age($d) and table_age($d) > $age ) {
        $pl->m('Skipping:', $t, 'because it is newer than', $age);
        next;
      }
      $pl->m('Operating on:', $t);
      if($rotate) {
        $r = undef;
        my $tr = TableRotater->new(
          $d,
          $rotate_format || DEFAULT_DATE_FORMAT,
          $dbh
        );
        my $ta = TableAge->new($d->get_dbh(1),
          ($d->get('r') || $d->get('t')) . ($rotate_format || DEFAULT_DATE_FORMAT));
        my $age = $ta->age_by_name($d->get('t'));
        if( $age ) {
          $pl->m('  Table looks already rotated for', $age);
        }
        else {
          $pl->m('  Rotating', 'to',
            $tr->date_rotate_name(
              $d->get('t'),
              $cur_date
            )
          );
          eval {
            # This modifies the table name in the DSN
            # So that the pack operation will work on the rotated name
            # if a rotation happened.
            if($tr->table_for_date($d->get('D'), $d->get('t'), $cur_date)) {
              $pl->m('  ..Table already rotated for', $cur_date);
              $d->{'t'}->{'value'} = $tr->date_rotate_name(
                $d->get('t'), $cur_date);
              $t = $d->get('t');
              die('Already rotated');
            }
            else {
              $d = rotate_table($tr, $d) unless($pretend);
              $t = $d->get('t');
            }
          };
          if($@ and $@ =~ /^Unable to create new table (.*?) at/) {
            $pl->e('  ..There was an error creating the replacement table.');
            $pl->e('  ..It is advised to manually examine the situation.');
            $pl->e(' .. DSN:', $d->str() ."\n",
              ' .. Temp table name:', $1);
            $pl->e('Exception:', $@);
            return 1;

          }
          elsif($@ and $@ =~ /^Failed to rename table to (.*), (.*) at/) {
            $pl->e('  ..There was an error renaming the tables.');
            $pl->e('  ..You must manually examine the situation.');
            $pl->e('  ..DSN:', $d->str() ."\n",
              ' ..Temp table name:', $2 . "\n",
              ' ..New table name:', $1);
            $pl->e('Exception:', $@);
            return 1;
          }
          elsif($@ and $@ =~ /^Already rotated/) { $pl->d('Redoing age evaluation.'); redo; }
          elsif($@) {
            $pl->e('Unknown exception:', $@);
          }
          else {
            $pl->m('  ..Rotated successfully.') unless($pretend);
            redo;
          }
        } # Else, table not rotated
      }
      if($pack) {
        $r = undef;
        $pl->m('  MyISAM Packing');
        eval {
          $r = pack_table($cfg->{'mysqld'}->{'datadir'}, $d, $t) unless($pretend);
        };
        $pl->d('Pack result:', 'Out:', $r->[0], 'Code:', $r->[1]);
        if($r and $r->[0] and $r->[0] =~ /already/) {
          $pl->d('  ..table already compressed.');
        }
        elsif($r and $r->[0] and $r->[0] =~ /error/i) {
          $pl->m('  ..encountered error.');
          $pl->e(' ', $r->[0], 'code:', $r->[1]);
        }
        elsif($@) {
          $pl->m('  ..encountered fatal error.');
          $pl->e(' ', $@);
          return 1;
        }
        elsif(!$pretend and $r) {
          $pl->m('  ..OK');
        }
      }
    }
  }

  return 0;
}

sub get_tables {
  my ($dsn) = @_;
  my $schema = $dsn->get('D');
  my $sql;
  my $regex;
  if($dsn->get('t')) {
    $sql = qq|SHOW TABLES FROM `$schema` LIKE '|. $dsn->get('t') ."'";
    $regex = $dsn->get('t');
  }
  elsif($dsn->get('r')) {
    $sql = qq|SHOW TABLES FROM `$schema`|;
    $regex = $dsn->get('r');
  }
  my @tbls = grep /^$regex$/,
  map { $_->[0] } @{$dsn->get_dbh(1)->selectall_arrayref($sql)};
  return \@tbls;
}

sub table_age {
  my ($dsn) = @_;
  my $ta = TableAge->new($dsn->get_dbh(1), $age_format);
  if($age_format eq 'createtime') {
    return $ta->age_by_status($dsn->get('D'), $dsn->get('t'));
  }
  else {
    my $reg = $dsn->get('r');
    return $ta->age_by_name(($dsn->get('t') =~ /^$reg$/));
  }
}

sub rotate_table {
  my ($tr, $dsn) = @_;
  $dsn->{'t'}->{'value'} = $tr->date_rotate(
    $dsn->get('D'),
    $dsn->get('t'),
    $cur_date
  );
  return $dsn;
}

sub pack_table {
  my ($datadir, $dsn) = @_;

  my $tp = TablePacker->new($dsn, $datadir);
  # If the table is not a myisam table - we convert it.
  if($tp->engine() ne 'myisam') {
    $tp->mk_myisam($0 . ' on ' . hostname());
  }
  if($tp->engine() eq 'myisam' and $tp->format() eq 'compressed') {
    unless($force) {
      return [0, 'Table '. $dsn->get('t') . ' already compressed.'];
    }
  }
  if($dsn->get('h') ne 'localhost') {
    my $ro = RObj->new($dsn->get('h'), $dsn->get('sU'), $dsn->get('sK'));
    # Make sure the RObj has the needed modules
    $ro->add_use('TablePacker', 'DBI');
    $ro->add_package('DSN');
    $ro->add_package('Which');
    $ro->add_package('TablePacker');
    $ro->add_main(sub {
        # This packs and checks the table specified by $dsn
        my ($self) = @_;
        eval {
          local $SIG{__DIE__};
          $self->pack();
          $self->check();
        };
        return $self;
      });
    $tp = [$ro->do($tp)]->[1];
  }
  else {
    eval {
      $tp->pack();
      $tp->check();
    };
  }
  # Flush the table so that mysql reloads the .FRM file.
  $tp->flush();
  chomp($tp->{errstr}) if($tp->{errstr});
  return [$tp->{errstr}, $tp->{errval}];
}

if(!caller) { exit(main(@ARGV)); }

1;

=pod

=head1 NAME

pdb-packer - Rotate and Compress tables.

=head1 SYNOPSIS

pdb-packer [options] DSN ...

=head1 DSN

A Maatkit style DSN.

Keys: h,u,p,F,sU,sK,rF,r,t,D

  h - host
  u - mysql user
  p - mysql password
  F - mysql defaults file
  sU - ssh user
  sK - ssh key
  rF - remote mysql defaults file
  r - table regex
  t - table name
  D - schema

The C<'r'> key is a perl regex to match table names against.
It MUST have exactly one capture group which selects exactly
the L<--age-format> portion of the table name for when L<--age> is used.

Example:

  table_name(_\d+)

=head1 OPTIONS

=over 4

=item B<--pretend>

Don't actually do anything. Just report what would happen.

=item B<--logfile=path>

Where to write out logfile. Normally messages just go to the console
and then are thrown away. However, by specifying the path to a file, or a
string starting such as syslog:LOCAL0 it's possible to also log to a
file or syslog.

Default: /dev/null

=item B<--rotate>

If passed, then the named/matched tables will be renamed to include a datestamp.

Default: (off)

=item B<--age>

Tables older than B<--age> will be operated on.

You can specify a string like: C<XX[dwmy]> where XX is a number,
C<d> is days, C<w> is weeks, C<m> is months, and C<y> is years.

The suffixes are case-insensitive.

Examples:

  --age 4d  # Tables older than 4 days
  --age 1m  # Tables older than 1 month
  --age 2W  # Tables older than 2 weeks

Default: (none)

=item B<--rotate-format>

The value is the format to append to the table name. If no value
is passed, or if an empty value is used, then it defaults to:
C<'_%Y%m%d'> (4-digit year, 2 digit month, 2 digit day).

See C<strftime(3)> for all possible formatting codes.

Example:
    table name: testbl
    rotated name: testbl_20100317

It's not currently possible to post or pre-date tables
- rotation always dates based on the time when the tool started.
So, even if the tool starts at 23:59 on 2010-03-17 and runs till 
01:01 on 2010-03-18, all rotated tables will be dated 2010-03-17.

Default: C<_%Y%m%d>

=item B<--age-format>

Selects the method for determining table age.

There are two different methods for determining when a table was created.

=over 8

=item createtime

This is the C<Created_At> property of a table as recorded by MySQL.
Unfortunately, this property is reset if an C<ALTER TABLE> is done,
even a trivial one. Which is why this is not the default.

=item datestamp

This method uses a datestamp after the table name.

It defaults to: C<'_%Y%m%d'> so that C<'testtbl_20100317'> is interpreted
to have been created on 03/17/2010 at 00:00:01.

This method is the default.

=back

This option only needs to be specified when L<--age> is specified and 
the default format is insufficient for some reason. To use the datestamp
method the value of this option should be a string with C<strftime(3)> flags.
To use the createtime method, the value of this option should be C<'createtime'>.

Default: C<_%Y%m%d>

=item B<--pack>

If passed, then, matched tables will be converted to packed myisam tables.

Default: off

=item B<--force>

Force packing to run, even if mysql thinks the table is already packed.

=back

=cut
