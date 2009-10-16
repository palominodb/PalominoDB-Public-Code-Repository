Feature: 'writefor' and 'readfor' keys must be present.
How can we know what server is doing what, unless
'writefor' and 'readfor' are present?

  Scenario Outline: writefor and readfor keys
    Given a dsn from file <case>
    Then validate should <result>

  Examples:
    | case                             | result            |
    | readfor_key_missing.yml          | raise SyntaxError |
    | writefor_key_missing.yml         | raise SyntaxError |
    | readfor_and_writefor_present.yml | pass              |

