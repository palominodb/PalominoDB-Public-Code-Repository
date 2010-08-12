node "db0.example.com" {
  include puppet::client
  include mysql::config
}
