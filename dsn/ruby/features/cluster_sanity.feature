Feature: Cluster sanity
Servers and clusters must agree on who is participating
with whom. If a cluster says it owns a server,
and the server doesn't agree, that's a problem.

  Scenario Outline: Cluster mismatch
    Given a dsn from file <case>
    Then validate raises SemanticsError with type <result>

    Examples:
      | case                             | result           |
      | cluster_mismatch_in_servers.yml  | ClusterMismatch  |
      | cluster_mismatch_on_primary.yml  | PrimaryMismatch  |
      | cluster_mismatch_on_failover.yml | FailoverMismatch |
