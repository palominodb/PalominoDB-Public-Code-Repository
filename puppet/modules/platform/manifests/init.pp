# Platform contains variables and defines to smooth over
# platform differences
class platform {

## Platform variables to make things more platform agnostic
  # FreeBSD and Linux have different ideas about the root
  # user group. Use this wherever you need 
  $root_user_group = $operatingsystem ? {
    'freebsd' => 'wheel',
    default   => 'root',
  }
}
