Feature: Server sanity
Servers should not reference unknown clusters.
This could easily be from a typo by someone in a rush.

  Scenario: Unknown cluster
    Given a dsn from file unknown_cluster.yml
    Then validate raises SemanticsError with type UnknownCluster
