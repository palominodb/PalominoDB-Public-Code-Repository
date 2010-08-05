#!/usr/bin/env perl
# Copyright (c) 2009-2010, PalominoDB, Inc.
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
# 
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
# 
#   * Neither the name of PalominoDB, Inc. nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
use strict;
use warnings FATAL => 'all';
# ###########################################################################
# ProcessLog package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End ProcessLog package
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
# TableIndexes package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End TableIndexes package
# ###########################################################################

package pdb_munch;
use strict;
use warnings FATAL => 'all';

use Getopt::Long qw(:config no_ignore_case);
use Text::CSV_XS;
use Data::Dumper;
use DBI;
use Pod::Usage;

use ProcessLog;
use IniFile;
use DSN;
use TableIndexes;

my $default_spec =<<'EOF';
; This is the built-in spec provided with pdb-munch.
; All of the source values have been commented out, and you MUST
; uncomment them and fill them in with real values.

[name]
column-type = varchar
method      = roundrobin
; Uncomment the below if you've got a CSV of firstname,lastname
;source    = csv:names.csv
match1      = (\w+) (\w+)

[phonenumber]
column-type = varchar
method      = random
; Uncomment the below if you've got a CSV of phone numbers
;source     = csv:phone_numbers.csv

; These matches are in descreasing priority
; The capture group specifies which portion of the number
; to replace with seed data.
match1      = \d{3}-(\d{3}-\d{4})
match2      = \(\d{3}\) (\d{3}-\d{4})
match3      = \+?\d{1,3} \d{3} (\d{3} \d{4})
match4      = \+?\d{1,3} \d{3} (\d{3}-\d{4})

[email]
column-type = varchar
method = random
;source = random
; Assumes a pre-sanitized email address
match1 = (.*?)@.*

[address_line_one]
column-type = varchar
method      = random
;source      = csv:addresses_line_one.csv
method      = random

match1     = \s*(\d+) ([a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ?).*
match2     = \s*(\d+) ([a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ?).*
match3     = \s*(\d+) ([a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ?).*
match4     = \s*(\d+) ([a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ?).*
EOF

my %c;
my %spec;
my %conf;
my $pl;
my $cur_tbl;
my $db;
my $dsn;

my $rr_upd;
my $changed_rows;

# key => table name
# value => [table, id_column, cur_id, min_id, max_id]
my %resume_info;

sub main {
  @ARGV = @_;
  my $dsnp = DSNParser->default();
  my $tbl_indexer;
  %resume_info = ();

  %c = ('logfile' => "$0.log", 'batch-size' => 10_000, 'max-retries' => 1_000);
  %spec = ();
  %conf = ();
  $cur_tbl = undef;
  $dsn = undef;
  $db = undef;
  $rr_upd = 0;
  $changed_rows = 0;
  GetOptions(\%c,
    'help|h',
    'logfile|L=s',
    'dry-run|n',
    'dump-spec',
    'spec|s=s',
    'config|c=s',
    'batch-size|b=i',
    'limit=i',
    'max-retries=i',
    'resume|r=s'
  );

  if($c{'help'}) {
    pod2usage(-verbose => 99);
    return 1;
  }
  
  $pl = ProcessLog->new($0, $c{logfile});
  
  ## Dump the spec and exit, if requested.
  if($c{'dump-spec'}) {
    my $spec_fh;
    open($spec_fh, ">default_spec.conf") or die("Unable to open default_spec.conf for writing");
    print($spec_fh $default_spec);
    close($spec_fh);
    $pl->i("Dumped default spec to default_spec.conf");
    return 0;
  }
  
  if(not exists $c{spec}) {
    $pl->e("--spec required. Try --help.");
    return 1;
  }
  
  if(not exists $c{config}) {
    $pl->e("--config required. Try --help.");
    return 1;
  }
  
  ## Load the spec file into a hash
  %spec = IniFile::read_config($c{spec});
  if(not %spec) {
    $pl->e("Unable to load $c{spec}.");
    return 1;
  }
  
  ## Verify each datatype in the spec.
  foreach my $type (keys %spec) {
    next if ($type =~ /^__\w+__$/); # Skip control sections
    for my $c (qw(source column-type method)) {
      if(not defined($spec{$type}->{$c})) {
        $pl->e("$c is required for all data specs");
        return 1;
      }
    }
  }
  
  ## Load all the CSV and List data sources into the spec, directly.
  ## Load all 'module' sources by "require"ing the associated method.
  foreach my $type (keys %spec) {
    next if ($type =~ /^__\w+__$/); # Skip control sections
    my $src = $spec{$type}->{source};
    $pl->d("src:", $src);
    if($src =~ /^csv:(.*)/) {
      my $fh;
      my $csv = Text::CSV_XS->new({binary => 1});
      unless(open($fh, "<$1")) {
        $pl->e("Unable to open seed data: $1");
        return 1;
      }
      ## Assuming that the data is basically hand-generated and thus
      ## will not be gigantic.
      while(my $row = $csv->getline($fh)) {
        push( @{$spec{$type}->{data}}, $row );
      }
    }
    elsif($src =~ /^list:(.*)/) {
      $spec{$type}->{data} = [map { [$_] }split(/,/, $1)];
    }
    elsif($src =~ /^module:(.*)/) {
      $spec{$type}->{source} = "module";
      require "$1";
    }
  }
  
  ## Load the config file into a hash
  %conf = IniFile::read_config($c{config});
  if(not %conf) {
    $pl->e("Unable to load $c{config}");
    return 1;
  }
  
  ProcessLog::_PdbDEBUG >= 3 && $pl->d("Spec:", Dumper(\%spec));
  ProcessLog::_PdbDEBUG >= 3 && $pl->d("Config:", Dumper(\%conf));
  
  ## Get connection information out of the config file
  $dsn = $dsnp->parse($conf{connection}{dsn});
  $db  = $dsn->get('D');
  delete $conf{connection};
  $tbl_indexer = TableIndexes->new($dsn);
  
  load_resume($c{resume}) if($c{resume});

  foreach my $tbl (sort keys %conf) {
    $rr_upd = 0;
    $cur_tbl = $tbl;
    $resume_info{$cur_tbl} ||= [undef, undef, undef, undef];
    $pl->d("Table config:", $conf{$tbl});
    $tbl_indexer->walk_table(undef, $c{'batch-size'}, $resume_info{$cur_tbl}->[2], \&update_row, $db, $tbl);
  }

  $pl->i("Changed Rows:", $changed_rows);

  return 0;
}

sub generate_varchar {
  # the length of the random string to generate
  my $length_of_randomstring=shift;
  $length_of_randomstring ||= 5;

  my @chars=('a'..'z','A'..'Z','0'..'9','_');
  my $random_string;
  foreach (1..$length_of_randomstring) {
    # rand @chars will generate a random 
    # number between 0 and scalar @chars
    $random_string.=$chars[rand @chars];
  }
  return $random_string;
}

sub generate_num {
  my $power_up_to=shift;
  my $min_digits=shift;
  $power_up_to=1 if($power_up_to==0);
  $min_digits ||= $power_up_to;
  # $power_up_to == 0 is a number between 0-9
  # == 1 is 0-9
  # == 2 is 0-99
  # == 3 is 0-199
  # == 4 is 0-1999
  # etc.

  return sprintf("%0${min_digits}d", int(rand(10**$power_up_to)));
}

# Saves information about the progress of the muncher to the file
# specified by --resume
sub save_resume {
  my $res_fh;
  open($res_fh, ">$c{resume}") or die("Unable to open $c{resume}: $!");
  foreach my $tbl (sort keys %resume_info) {
    print($res_fh join("\t", ($tbl, @{$resume_info{$tbl}})));
  }
  close($res_fh);

  return 1;
}

# Loads information about the progress of the muncher from the file
# specified by --resume
sub load_resume {
  my $res_fh;
  my @res;
  open($res_fh, "<$c{resume}") or die("Unable to open $c{resume}: $!");
  while(<$res_fh>) {
    my ($tbl, @data) = split(/\t/);
    chomp(@data);
    $resume_info{$tbl} = [@data];
  }
}

## Does the actual work of updating rows to have obfuscated values.
sub update_row {
  my ($idx_col, $dbh, $min_idx, $max_idx, $row) = @_;
  my $dry_run = $c{'dry-run'};
  my $tbl_config = $conf{$cur_tbl};
  my $max_retries = $c{'max-retries'};
  my $retries = 0;
  ## @vals contains the updated column data after the COLUMN: loop
  ## @data contains the seed data, if any is present.
  my (@vals, $data);

# We jump to this label when there was a duplicate key error on the row
# and $c{'max-retries'} is greater than 0.
UPDATE_ROW_TOP:
  @vals = ();
  $data = [];

  ProcessLog::_PdbDEBUG >= 2 && $pl->d("Row:", "$idx_col >= $min_idx AND $idx_col <= $max_idx", Dumper($row));

  ## The keys are sorted here to force the same order in the query as in @vals
  ## Since, @vals is passed wholesale onto $sth->execute() later.
  my $sth = $dbh->prepare_cached("UPDATE `$db`.`$cur_tbl` SET ". join("=?, ", sort keys %$tbl_config) ."=? WHERE `$idx_col`=?");

  COLUMN: foreach my $col (sort keys %$tbl_config) {
    if(not defined($$row{$col})) {
      $row->{$col} = "";
    }
    ## Populate the @data array with either seed data from the pre-loaded CSV file
    ## Or a couple values from a random string generator.
    if($spec{$$tbl_config{$col}}->{source} eq "random") {
      $data = [ [generate_varchar(int(rand(length($row->{$col}))))],
      [generate_varchar(int(rand(length($row->{$col}))))],
      [generate_varchar(int(rand(length($row->{$col}))))] ];
    }
    elsif($spec{$$tbl_config{$col}}->{source} eq "module") {
      ## Nothing done here. This is to prevent the catch-all from running.
    }
    else {
      $data = $spec{$$tbl_config{$col}}->{data};
    }
    
    ## Select the data in the fashion requested.
    if($spec{$$tbl_config{$col}}->{method} eq 'random') {
      push @vals, $$data[int(rand(scalar(@$data)-1))];
    }
    elsif($spec{$$tbl_config{$col}}->{method} eq 'roundrobin') {
      push @vals, $$data[ $rr_upd % (scalar(@$data)-1) ];
      $rr_upd++;
    }
    elsif($spec{$$tbl_config{$col}}->{source} eq "module") {
      no strict 'refs';
      push @vals, &{$spec{$$tbl_config{$col}}->{method}}($dbh, $row->{$col});
      next COLUMN;
    }

    ## Keys are sorted here so that each of the matchN keys is in ascending order
    ## thus prioritising lower values of N.
    SEED_KEY: foreach my $sk (sort(grep(/match\d+/, keys(%{$spec{$$tbl_config{$col}}}))) ) {
      my $rgx = $spec{$$tbl_config{$col}}{$sk};
      my @res = $row->{$col} =~ /^$rgx$/;
      ProcessLog::_PdbDEBUG >= 2 && $pl->d("R:", $col, qr/^$rgx$/, $#res+1, @res);
      if(@res) {
        for(my $i=0; $i < $#res+1; $i++) {
          ProcessLog::_PdbDEBUG >= 2 && $pl->d("V:", $col, Dumper(\@vals));
          ProcessLog::_PdbDEBUG >= 2 && $pl->d("S:", $col, $res[$i], "(", @{$vals[-1]}, ")", "*", $vals[-1]->[$i], "*", $i, scalar @{$vals[-1]});
          substr($row->{$col}, index($row->{$col}, $res[$i]), length($res[$i]), $vals[-1]->[$i]);
        }
        $vals[-1] = $row->{$col};
        last SEED_KEY;
      }
    }
    if(ref($vals[-1])) {
      if($spec{'__params__'}{'die-on-unmatched'}) {
        die("Unable to match $col");
      }
      $vals[-1] = $row->{$col};
    }
  }
  ProcessLog::_PdbDEBUG >= 2 && $pl->d("SQL:", "UPDATE `$db`.`$cur_tbl` SET ". join("=?, ", sort keys %$tbl_config) ."=? WHERE `$idx_col`=?");
  ProcessLog::_PdbDEBUG >= 2 && $pl->d("SQL Bind:", @vals, $row->{$idx_col});
  eval {
    $sth->execute(@vals, $row->{$idx_col}) unless($dry_run);
  };
  if($@ and $@ =~ /.*Duplicate entry/) {
    if($max_retries and $retries < $max_retries) {
      $retries++;
      goto UPDATE_ROW_TOP;
    }
    else {
      die($@);
    }
  }
  if($changed_rows % $c{'batch-size'} == 0) {
    $pl->d("SQL: COMMIT /*", $changed_rows, '/', $c{'batch-size'}, "*/");
    $dbh->commit;
    $dbh->begin_work if($dbh->{AutoCommit});
    if($c{resume}) {
      $resume_info{$cur_tbl} = [$idx_col, $$row{$idx_col}, $min_idx, $max_idx];
      save_resume();
    }
  }
  $changed_rows++;
  if($c{'limit'} and ($changed_rows > $c{'limit'})) {
    die("Reached $c{'limit'} rows");
  }
}

if(!caller) { exit(main(@ARGV)); }

=pod

=head1 NAME

pdb-munch - Flexible data obfuscation tool

=head1 SYNOPSIS

This tool was made to santize records in a table so that
taking it out of the secure environment in which it was created
is feasible. This can be useful for devs who want a copy of "real"
data to take home with them for testing purposes.

This tool is designed to modify your data. B<DO NOT> run it on
production systems. Because it does destructive operations on your
data, it does not accept hostnames on the commandline. Always double
and triple check your configuration before running this tool.

=head1 OPTIONS

=over 8

=item --logfile,-L

Specifies where to log. Can be set to a string like: syslog:<facility> to
do syslog logging. LOCAL0 usually logs to /var/log/messages.

Default: ./pdb-munch.log

=item --dry-run,-n

Only report on what actions would be taken.

=item --dump-spec

Dump the built-in spec file to the file F<default_spec.cnf>.

This is a good starting point for building your own spec file.

=item --spec,-s

Use the column types from this file.

=item --config,-c

Use the host/table configuration in this file.

=item --max-retries

How many times to retry after a unique key error.

Default: 1,000

=item --limit

If used, will stop the tool after --limit rows.

=item --batch-size,-b

Tells pdb-munch to modify --batch-size records at a time.
The batch size also determines the commit interval. For InnoDB,
very long transactions can push other operations out and slow
down the muncher.

Default: 10,000

=item --resume,-r

Load saved resume info from file.
In order to start pdb-munch for a new 

=back

=head1 EXAMPLES

  # Munch the data on test_machine using
  # The default column type specs.
  pdb-munch -d default_spec.conf -c test_machine.conf
  
  # Dumps the built-in spec to default_spec.conf
  # This is a very handy starting point, since the default
  # munch spec includes several datatypes. 
  pdb-munch --dump-spec
  
=head1 CONFIG FILES

The config file defines which host to connect to, and what columns in which
tables to modify. Example:

  [connection]
  dsn =   h=testdb,u=root,p=pass,D=testdb
  
  ;; Tables
  ;[table_name]
  ;column_name = type
  
  [addresses]
  address_line_one = address
  name = name
  email = email_righthand
  
The C<connection> section has only one parameter: C<dsn>, it specifies
the connection information. It's a list of key-value pairs separated by
commas. Description of keys:

  h - host name
  u - mysql user
  p - mysql password
  D - mysql schema(database)

All are mandatory.
  
=head1 SPEC FILES

Spec files describe different kinds of datatypes stored inside
a mysql column (for instance, an address stored in a varchar column).
They also describe how to modify the data contained in those columns.

A spec file is an Ini style configuration file, composed of one or more
sections following the form:

  [<datatype>]
  column-type = <mysql column type>
  source      = <csv:<file>|list:<comma separated list>|random|module:<file>>
  method      = <random|roundrobin|<perl subroutine name>>
  match<I>    = <perl regex>
  match<I+1>  = <perl regex>
  match<I+N>  = <perl regex>

The spec file should contain the special section C<[__param__]> which controls
the way certain conditions in the muncher are handled. Presently, only one
parameter C<die-on-unmatched> is used, which, if set to a true value will
cause pdb-munch to die if all of the C<< match<I> >> patterns fail. Example:

  [__param__]
  die-on-unmatched = 1

Parameter descriptions:

=over 8

=item C<column-type>

Specifies the type of MySQL column this datatype is stored in.
Presently, this is unused, but required.

=item C<source>

The source is where to pull data from. The most common is from a CSV file.

The C<list:> type is an inline comma separated list of values. It's most
useful for ENUM column types.

C<random> generates several randomly sized random strings per row and selects one.

C<module:> is the most flexible, it allows you to load an arbitrary perl module
and then use the C<method> parameter to call a subroutine in it. The sub will
recieve a handle to the database connection, and the column data.

=item C<method>

One of: random, roundrobin, or the name of a perl subroutine.
For random, the tool will select a random value from the source, for roundrobin,
it'll use them in order as they appear one after another in a loop.
The perl sub, if used, will be called as described above.

=item C<< match<I> >>

For the C<csv:>, C<list:>, and C<random> source types, these define how to
replace the cell contents. For C<csv:> types, each capture group in the regex
corresponds to a column in the CSV. The C<list:> and C<random> types ony support
one capture group.

=back

=head1 ENVIRONMENT

Like all PalominoDB data management tools, this tool responds to the
environment variable C<Pdb_DEBUG>, when it is set, this tool produces
copious amounts of debugging output.

=cut

1;
