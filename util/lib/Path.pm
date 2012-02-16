package Path;
use File::Find;

sub dir_empty {
  my $dir = shift;
  eval "use File::Find;";
  my $rmtree_sub = sub {
    if(-d $File::Find::name && $File::Find::name ne $dir) {
      rmdir $File::Find::name or die('rmtree: unable to remove directory '. $File::Find::name);
    }
    elsif($_ ne $dir) {
      unlink $File::Find::name or die('rmtree: unable to delete file '. $File::Find::name);
    }
    elsif($_ eq $dir) {
      return;
    }
    else {
      die('rmtree: unexpected error when attempting to remove ' . $File::Find::name);
    }
  };
  find( { wanted => $rmtree_sub, no_chdir => 1, bydepth => 1 }, $dir );

  return 0;
}
1;
