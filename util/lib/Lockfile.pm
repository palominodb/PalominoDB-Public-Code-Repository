# Lockfile.pm - creates a simple lockfile object.
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

package Lockfile;
use strict;
use warnings FATAL => 'all';
use Fcntl qw(:DEFAULT :flock);

sub get {
	my ($class, $file, $timeout) = @_;
	my $self = {};
	my $lock_fh;
	if( not defined $timeout ) {
		$timeout = 0;
	}
	if( not defined $file ) {
		die("Lockfile: Missing mandatory argument: file\n");
	}
	eval {
		my $flocked = 0;
		local $SIG{'ALRM'} = sub { die("timeout.\n"); };
		if( $timeout ) {
			alarm($timeout);
		}
		# Attempt indefinitely (up to alarm time) to get a lock
		# that also has an existing file.
		while(!$flocked) {
			sysopen($lock_fh, $file, O_RDWR | O_CREAT) or die("failed to open $file: $!\n");
			flock($lock_fh, LOCK_EX) or die("failed to flock $file: $!\n");
			if(! -f $file) {
				close($file);
			}
			$flocked = 1;
		}
		alarm(0) if( $timeout );
	};
	if($@ and $@ =~ /timeout/) {
		alarm(0) if( $timeout );
		die("Lockfile: Unable to acquire lock on $file after $timeout seconds");
	}
	elsif($@ and $@ =~ /failed to open/) {
		alarm(0) if( $timeout );
		die("Lockfile: Unable to open lock $file");
	}
	elsif($@ and $@ =~ /failed to flock/) {
		alarm(0) if( $timeout );
		die("Lockfile: Unable to flock $file");
	}
	elsif($@) {
		chomp($_ = "$@");
		alarm(0) if( $timeout );
		die("Lockfile: Unknown error: $_");
	}
	$$self{'file'} = $file;
	$$self{'timeout'} =  $timeout;
	$$self{'fh'} = $lock_fh;
	bless $self, $class;
	return $self;
}

sub DESTROY {
	my ($self) = @_;
	my $fh = $$self{'fh'};

	flock($fh, LOCK_UN) or die("Lockfile: Unable to unlock $$self{'file'}");
	close($fh) or die("Lockfile: Unable to close $$self{'file'}: $!");
	unlink($$self{'file'}) or die("Lockfile: Unable to remove lock $$self{'file'}: $!");
}

=pod

=head1 NAME

Lockfile - A simple lockfile object.

=head1 SYNOPSIS

Usage is simple. Create the Lockfile object and hang onto it until you don't
need to have your lock anymore. It'll be unlocked when it goes out of scope.
All locks are exlcusive. By default it'll wait indefinitely for the lock.

Example:

	{
		my $l = Lockfile->get("my.lock", 5);
		# wait up to 5 seconds for a lock, and then
		# do some actions protected by a lock.
	}
	# no longer protected by the lock.

=head1 METHODS

=head2 get

	my $handle = Lockfile->get($file);
	my $handle = Lockfile->get($file, $timeout);

Gets an exclusive lock on C<$file>, optionally waiting for C<$timeout> seconds.
You must hold onto the returned object for as long as you want the lock to be
held. The file is unlocked and removed as soon as the object goes out of scope.
If C<$timeout> is not specified, then C<get> will wait indefinitely for a lock.

=cut

1;
