Feature: Error on Empty DSN
Empty DSN's should throw an error. They are bad.

  Scenario:  Empty DSN file
    Given an empty dsn
    Then validate raises SemanticsError with type EmptyDSN

  Scenario:  Empty DSN from hash
    Given an empty dsn
    Then validate raises SemanticsError with type EmptyDSN
