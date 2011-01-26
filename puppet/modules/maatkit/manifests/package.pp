class maatkit::package {
  $maatkit_version = $maatkit_version ? {
    '' => '5240-1',
    default => $maatkit_version,
  }
  package { 'maatkit':
    ensure => $maatkit_version,
  }
}
