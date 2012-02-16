Feature: 'version' should be a required key for servers.
Having the mysql version will make laine happy.

  Scenario: version key is missing
    Given a dsn from file server_missing_version.yml
    Then validate should raise SyntaxError

  Scenario: version key is present
    Given a dsn from file server_present_version.yml
    Then validate should pass
