#!/usr/bin/perl -w
### work around for ePn until I can refactor completely.
# nagios: -epn
###
# Copyright (c) 2009-2011, PalominoDB, Inc.
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
$| = 1;
our $VERSION = "1.1.4";

use strict;
use Nagios::Plugin;
use DBI;
use Storable;
use Data::Dumper;
use Carp;
use File::stat;
use Switch;
use Fcntl qw(:flock);
#############################################################################
our $VERBOSE = 0;

###### MAIN ######
my ($np, $dbh) = init_plugin();
my $meta_data = load_meta_data();

switch ($np->opts->mode)
{
  case 'long-query'
  {
    foreach my $query_hr (@{$meta_data->{proc_list}})
    {
      # this is where we can add rules to skip specific queries or users
      next if($query_hr->{User} =~ /system user/);
      next if($query_hr->{Command} =~ /Binlog Dump/);
      next unless($query_hr->{Info});
      ###
      my $code = $np->check_threshold(check => $query_hr->{Time}, warning => $np->opts->warning, critical => $np->opts->critical);
      if($code)
      {
        my $msg = sprintf("Query running for %d seconds: %s", $query_hr->{Time}, $query_hr->{Info});
        $np->add_message($code, $msg);
        last;
      }
    }
  }
  case "locked-query"
  {
    foreach my $query_hr (@{$meta_data->{proc_list}})
    {
      if($query_hr->{Command} eq 'Query' && (defined($query_hr->{State}) && $query_hr->{State} eq 'Locked'))
      {
        my $code = $np->check_threshold(check => $query_hr->{Time}, warning => $np->opts->warning, critical => $np->opts->critical);
        if($code)
        {
          my $msg = sprintf("Locked query detected: %s", $query_hr->{Info});
          $np->add_message($code, $msg);
          last;
        }
      } 
    }
  }
  case "varcomp" 
  { 
    my ($comp_res, $expr_res) = mode_varcomp($meta_data);
    if($comp_res)
    {
      my $msg;
      if ($np->opts->comparison) {
        $msg = sprintf("Comparison check failed: (%s)   %s   = %s", $np->opts->expression, $np->opts->comparison, $expr_res);
      } else {
        $msg = sprintf("Comparison check failed: (%s)   %s   %s   = %s", $np->opts->expression, $np->opts->warning, $np->opts->critical, $expr_res);
        }
      $np->add_message($comp_res, $msg);
    }
    else
    {
      my $msg;
      if ($np->opts->comparison) {
        $msg = sprintf("Comparison check passed: (%s)   %s   = %s", $np->opts->expression, $np->opts->comparison, $expr_res);
      } else {
        $msg = sprintf("Comparison check passed: (%s)   %s   %s   = %s", $np->opts->expression, $np->opts->warning, $np->opts->critical, $expr_res);
      }
      $np->add_message(OK, $msg);
    }
  }
  else 
  { 
    $np->nagios_die("Unknown run mode.");
  }
}

cleanup();

my  $exit_code = OK;
my  $exit_msg = 'All clear';
($exit_code, $exit_msg) = $np->check_messages;

$np->nagios_exit($exit_code, $exit_msg);

##### END OF MAIN ######

#####
# run mode 'varcomp' looks at a variable from SHOW GLOBAL VARS and SHOW STATUS and does a calculation and comparison.
#####
sub mode_varcomp
{
  my $meta_data = shift;
  my $parsed_expr = '';
  my $expr_res = '';
  my $comp_res = '';

  if ($np->opts->expression) { pdebug("expr (" . $np->opts->expression . ")\n"); }
  if ($np->opts->comparison) { pdebug("comp (" . $np->opts->comparison . ")\n"); }
  if ($np->opts->warning) { pdebug("comp (" . $np->opts->warning . ")\n"); }
  if ($np->opts->critical) { pdebug("comp (" . $np->opts->critical . ")\n"); }

  my $do_math = 0;
  foreach my $word (split(/\b/, $np->opts->expression)) 
  {
    pdebug("parsing $word\n");
    if(exists($meta_data->{varstatus}->{$word})) 
    {
      pdebug("found $word in metadata, value is $meta_data->{varstatus}->{$word} \n");
      $parsed_expr .= $meta_data->{varstatus}->{$word};
    }
    elsif($word =~ m%(\*|\+|\-|/|\.|\(|\)|\%\ |\d)%)
    {
      $do_math = 1;
      pdebug("doing math because $1 is present in $word\n");
      $parsed_expr .= $word;
    }
    else
    {
      $np->nagios_die("Unknown variable '$word'");
    }
  }
  if($do_math)
  {
    pdebug("doing math: $parsed_expr\n");
    $expr_res = eval($parsed_expr); 
    #if it's a float, round to two decimal places.
    $expr_res = nearest(.01, $expr_res) if($expr_res =~ /\d+\.?\d*/);
  }
  else
  {
    $expr_res = "'$parsed_expr'";
  }
  
  if ($np->opts->comparison) {
    my $comp_expr = $expr_res . $np->opts->comparison;
    $comp_res = eval($comp_expr);  

    pdebug("Parsed ($parsed_expr) = ($expr_res) | ($comp_expr) = ($comp_res)\n");
    return $comp_res, $expr_res;
  }
  if ($np->opts->warning && $np->opts->critical) {
    my $code=3;
    my $comp_expr_warn = $expr_res . $np->opts->warning;
    my $comp_expr_crit = $expr_res . $np->opts->critical;
    my $comp_res_crit = eval($comp_expr_crit) || 0;  
    my $comp_res_warn = eval($comp_expr_warn) || 0;  
    if ($comp_res_crit==0 && $comp_res_warn==0) { $code=0; }
    elsif ($comp_res_crit==1) { $code=2; }
    elsif ($comp_res_warn==1) { $code=1; }
    return $code, $expr_res;
  }
}

#####
# Figure out where / how to get server meta data
#####
sub load_meta_data
{
  my $data;

  if($np->opts->no_cache)
  {
    $data = fetch_server_meta_data();
  }
  else
  {
    $data = get_from_cache();
    unless($data)
    {
      $data = fetch_server_meta_data();
      write_cache($data);
    }
  } 
  return $data;
}

#####
# Gather all of the good info from mysql, stuff it into a nice hash and return a reference
#####
sub fetch_server_meta_data
{
  my %meta_data;
  pdebug("Getting meta data...\n");
  pdebug("\tSHOW FULL PROCESSLIST...\n");
  $meta_data{proc_list} = dbi_exec_for_loh($dbh, q|SHOW FULL PROCESSLIST|);     
#  pdebug("\tSHOW ENGINE INNODB STATUS...\n");
#  $meta_data{innodb_status} = $dbh->selectall_arrayref(q|SHOW ENGINE INNODB STATUS|);
  pdebug("\tSHOW GLOBAL VARIABLES...\n");
  $meta_data{varstatus} = dbi_exec_for_paired_hash($dbh, q|SHOW GLOBAL VARIABLES|);
  pdebug("\tSHOW GLOBAL STATUS...\n");
  $meta_data{varstatus} = hash_merge($meta_data{varstatus}, dbi_exec_for_paired_hash($dbh, q|SHOW GLOBAL STATUS|));
=item
  # need to add --collect_slave_info option
  pdebug("\tSHOW MASTER STATUS...\n");
  $meta_data{master_status} = dbi_exec_for_paired_hash($dbh, q|SHOW MASTER STATUS|);
  pdebug("\tSHOW SLAVE STATUS...\n");
  $meta_data{slave_status} = dbi_exec_for_paired_hash($dbh, q|SHOW SLAVE STATUS|);
=cut
#  print Dumper(%meta_data);
  return \%meta_data;
}

#####
# Check to make sure the cache file on disk is fresh, if not, kill it and repopulate
#####
sub get_from_cache
{
  my $cache_info = cache_paths();
  my $data;

  if(-e $cache_info->{'file'})
  {
     my $cf_stat = stat($cache_info->{'file'}); 
     my $cf_age = time() - $cf_stat->mtime;
     pdebug("Cache file ($cache_info->{'file'}) is $cf_age second old - max age is " . $np->opts->max_cache_age . ".\n");
     # if the file is too old, lock, if we can't get a lock, stale data it is becuase another process is refreshing
     my $has_lock = lock_cache();
     if($cf_age >= $np->opts->max_cache_age && $has_lock)
     {
       # we can lock, so refresh
       $data = refresh_cache();
     }
     else
     {
       pdebug("Using cache file ($cache_info->{'file'})...\n");
       $data = retrieve($cache_info->{'file'});
     }
  }

  return $data; 
}

sub refresh_cache
{
  my $cache_info = cache_paths();
  pdebug("Cache file ($cache_info->{'file'}) is too old refreshing...\n");
  my $data = fetch_server_meta_data();
  write_cache($data);
  unlock_cache();
  return $data;
}

sub cache_paths
{
  my %cache_info;
  $cache_info{'dir'} = $np->opts->cache_dir;
  $cache_info{'file'} = sprintf("%s/%s-%s.cache", $cache_info{dir}, $np->opts->hostname, $np->opts->port);
  $cache_info{'tmp_file'} = sprintf("%s/%s-%s.cache.%s", $cache_info{dir}, $np->opts->hostname, $np->opts->port, $$);
  return \%cache_info;
}

#####
# Serialize the cache and write it out to a file
#####
sub write_cache
{
  pdebug("Writing meta data cache to file...\n");
  my $data = shift;
  my $cache_info = cache_paths();
  pdebug("Cache paths: $cache_info->{'dir'}, $cache_info->{'file'}\n");
  unless(-d $cache_info->{dir}) { mkdir($cache_info->{dir}) || $np->nagios_die(CRITICAL, "Can't create cache directory : $!") }
  store($data, $cache_info->{'tmp_file'}) || $np->nagios_die(CRITICAL, "Can't write cache file: $!");
  rename($cache_info->{'tmp_file'}, $cache_info->{'file'}) || $np->nagios_die(CRITICAL, "Can't rename cache file: $!");
  return $data;
}

sub lock_cache
{
  my $cache_info = cache_paths();
  open(CACHE_FILE, "<$cache_info->{'file'}") || $np->nagios_die(UNKNOWN, "Can't open cache for locking: $!\n");
  pdebug("Locking ($cache_info->{'file'})...\n");
  my $lock_res = flock(CACHE_FILE, LOCK_EX|LOCK_NB); 
  pdebug("LOCK: ($lock_res)\n");
  return $lock_res;
}


sub unlock_cache
{
  my $cache_info = cache_paths();
  flock(CACHE_FILE, LOCK_UN);
  close(CACHE_FILE);
}

sub cleanup
{
  $dbh->disconnect;
}

# connect to database server and return a DBI handle
sub dbi_connect 
{
  my $np = shift;
  
  my $dsn = sprintf("DBI:mysql:database=%s:host=%s:port=%d;mysql_read_default_file=$ENV{HOME}/.my.cnf", $np->opts->database, $np->opts->hostname, $np->opts->port);
  my $dbh = DBI->connect($dsn, $np->opts->user, $np->opts->password, { RaiseError => 0, AutoCommit => 1 });
  unless($dbh) { $np->nagios_exit(CRITICAL, "Can't connect to MySQL: $DBI::errstr") }
  return $dbh;
}

sub pdebug
{
  my $msg = shift;
  print $msg if($VERBOSE >= 2);
}

sub init_plugin
{
  my $np = Nagios::Plugin->new(
	usage => "Usage: %s [-v|-verbose] [-H|--hostname <host>] [-P|--port <port>] [-u|--user <user>] [-p|--password <passwd>] [-d|--database <database>] [-w|--warning <threshold>] [-c|--critical <threshold>] [--shortname <shortname>] [-m|--mode <varcomp|locked-query|long-query>] [--cache_dir <directory>] [--no_cache] [--max_cache_age <seconds>] [--comparison <math expression>] [--expression <math expression>] ",
	version => $VERSION,
	license => "Copyright (c) 2009-2010, PalominoDB, Inc.",
);

  $np->add_arg(spec => 'hostname|H=s', default => 'localhost', help => "-H, --hostname\n\tMySQL server hostname");
  $np->add_arg(spec => 'port|P=i', default => 3306, help => "-P, --port\n\tMySQL server port");
  $np->add_arg(spec => 'user|u=s', required => 0, help => "-u, --user\n\tMySQL username");
  $np->add_arg(spec => 'password|p=s', required => 0, help => "-p, --password\n\tMySQL password");
  $np->add_arg(spec => 'database|d=s', required => 0, default => '', help => "-d, --database\n\tMySQL database");
  $np->add_arg(spec => 'warning|w=s', required => 0, default => '', help => "-w, --warning\n\tWarning Threshold");
  $np->add_arg(spec => 'critical|c=s', required => 0, default => '', help => "-c, --critical\n\tCritical Threshold");
  $np->add_arg(spec => 'shortname=s', required => 0, default => '', help => qq|--shortname\n\tName/Label to give this check i.e "max_connections"|);
  $np->add_arg(spec => 'mode|m=s', required => 1, help => 
    qq|-m, --mode\n\tRun mode\n| . 
    qq|\t\tvarcomp\t\tVariable comparison (--comparison and --expression)\n| . 
    qq|\t\tlocked-query\tCheck for queries in the 'Locked' state\n| .
    qq|\t\tlong-query\tCheck for long running queries\n|
  );
  $np->add_arg(spec => 'cache_dir=s', required => 0, default => "/tmp/pdb_nagios_cache", help => "-d, --database\n\tMySQL database");
  $np->add_arg(spec => 'no_cache', required => 0, help => "--no_cache\n\tIgnore var/processlist cache");
  $np->add_arg(spec => 'max_cache_age=i', required => 0, default => 300, help => "--max_cache_age\n\tNumber of seconds before the meta data cache is considered stale and refreshed");
  $np->add_arg(spec => 'comparison=s', required => 0, help => qq|--comparison\n\tComparison and threshold (Perl syntax), e.g. ">80"|);
  $np->add_arg(spec => 'expression=s', required => 0, help => qq|--expression\n\tThe calculation, a Perl expression with MySQL variable names|);

  # process the command line args...
  $np->getopts;
  $VERBOSE = $np->opts->verbose;
  $np->shortname($np->opts->shortname);
  pdebug("VERBOSE IS $VERBOSE / Shortname is " . $np->shortname . " ...\n");

  switch ($np->opts->mode)
  {
    case "long-query"
    {
      $np->shortname('mysql_long-query') unless($np->opts->shortname);
      unless($np->opts->warning && $np->opts->critical)
      {
        $np->nagios_die("ERROR: run mode 'long-query' requires --warning and --critical params.");
      }
    }
    case "locked-query"
    {
      $np->shortname('mysql_locked-query') unless($np->opts->shortname);
    }
    case "varcomp"
    {
      $np->shortname('mysql_varcomp') unless($np->opts->shortname);
      unless(($np->opts->comparison && $np->opts->expression && !$np->opts->warning && !$np->opts->critical) || ($np->opts->expression && $np->opts->warning && $np->opts->critical && !$np->opts->comparison) )
      {
       $np->nagios_die("ERROR: run mode 'varcomp' requires --expression and EITHER --comparison OR --warning and --critical params.");
      }
    }    
  } 

  return $np, $dbh = dbi_connect($np);
}

sub dbi_exec_for_paired_hash { dbi_fetch_paired_hash (prepare_and_exec(@_)) }
sub dbi_exec_for_loh { dbi_fetch_loh (prepare_and_exec(@_)) }

sub dbi_fetch_loh 
{ 
  my $sth = shift;
  my $lref = $sth->fetchall_arrayref( {} );
  return \@{$lref};
}

sub dbi_fetch_paired_hash 
{
  my $sth = shift;
  die "query asks for " . $sth->{NUM_OF_FIELDS} . " fields, not 2" if $sth->{NUM_OF_FIELDS} != 2;
  my $key = $sth->{NAME}->[0];
  my %hash;
  while (my $lref = $sth->fetchrow_arrayref) 
  {
    my $val = $lref->[0];
    $hash{$val} = $lref->[1];
  }
  return \%hash;
}

sub prepare_and_exec 
{
  my $dbh = shift;
  my $flags = 0;
  $flags = shift if ($_[0] =~ /^\d+$/);
  my ($query, @binds) = @_;
  my $sth = (($flags & DBI_CACHE_STMT()) ? $dbh->prepare_cached($query) : $dbh->prepare($query)) 
	|| croak "error preparing query: ".$dbh->errstr." ($query)";
  $sth->execute(@binds) || croak "error executing query: ".$dbh->errstr." ($query)";
  return $sth;
}

sub DBI_CACHE_STMT { 1 }

sub hash_merge 
{
  shift unless ref $_[0]; # Take care of the case we're called like Hash::Merge::Simple->merge(...)
  my ($left, @right) = @_;
  return $left unless @right;
  return hash_merge($left, hash_merge(@right)) if @right > 1;
  my ($right) = @right;
  my %merge = %$left;

  for my $key (keys %$right) 
  {
    my ($hr, $hl) = map { ref $_->{$key} eq 'HASH' } $right, $left;
    if ($hr and $hl)
    {
      $merge{$key} = hash_merge($left->{$key}, $right->{$key});
    }
    else 
    {
      $merge{$key} = $right->{$key};
    }
  }
  return \%merge;
}

# stolen from Math::Round
sub nearest 
{
  my $targ = abs(shift);
  my @res  = map {
  if ($_ >= 0) { $targ * int(($_ + 0.50000000000008 * $targ) / $targ); }
     else { $targ * POSIX::ceil(($_ - 0.50000000000008 * $targ) / $targ); }
  } @_;

  return (wantarray) ? @res : $res[0];
}

1;
