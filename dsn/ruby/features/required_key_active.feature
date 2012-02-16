Feature: 'active' should be a required key.
If all servers and clusters don't have 'active', then
how can we know if our application can trust them?

  Scenario: Server missing active key
    Given a dsn from file server_missing_active_key.yml
    Then validate should raise SyntaxError

  Scenario: Cluster missing active key
    Given a dsn from file cluster_missing_active_key.yml
    Then validate should raise SyntaxError
