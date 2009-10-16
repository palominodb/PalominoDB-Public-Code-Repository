Feature: 'primary' and 'failover' should be required keys.
M/M pairs need a primary and a failover.
validate should fail if either of these keys is missing
from any cluster.

  Scenario Outline: primary and failover keys
    Given a dsn from file <case>
    Then validate should <result>

    Examples:
      | case                             | result            |
      | primary_missing.yml              | raise SyntaxError |
      | failover_missing.yml             | raise SyntaxError |
      | primary_and_failover_present.yml | pass              |
