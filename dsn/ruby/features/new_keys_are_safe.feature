Feature: New keys are safe.
From time to time, a client, or project specific
key must be added to the dsn. Those keys must
not break the dsn.

  Scenario: New key
    Given a dsn from file new_keys.yml
    Then validate should pass
    And getting the key frozbit from server s1 should return 10d
