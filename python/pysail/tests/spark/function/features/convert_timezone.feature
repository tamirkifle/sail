@convert_timezone
Feature: timestamp conversion functions respect session timezone

  The functions convert_timezone, from_utc_timestamp, to_utc_timestamp, and
  make_timestamp capture spark.sql.session.timeZone at planning time and must
  use it for timestamp interpretation and display — not the host OS timezone.

  Background:
    Given config spark.sql.session.timeZone = America/New_York

  # America/New_York is UTC-5 in January (standard time, EST).
  # America/New_York is UTC-4 in July  (daylight saving time, EDT).
  # Reference instants used below:
  #   2024-01-15 10:00:00 NY (EST)  =  2024-01-15 15:00:00 UTC
  #   2024-01-15 10:30:00 LA (PST)  =  2024-01-15 18:30:00 UTC  (LA = UTC-8 in Jan)
  #   2024-07-15 10:00:00 NY (EDT)  =  2024-07-15 14:00:00 UTC

  Rule: make_timestamp interprets components in session timezone

    Scenario: 6-arg make_timestamp — components are in session timezone
      When query
      """
      SELECT CAST(make_timestamp(2024, 1, 15, 10, 0, 0.0) AS STRING) AS result
      """
      Then query result
      | result              |
      | 2024-01-15 10:00:00 |

    Scenario: 2-arg make_timestamp(date, time) — combined in session timezone
      When query
      """
      SELECT CAST(make_timestamp(DATE '2024-01-15', TIME '10:00:00') AS STRING) AS result
      """
      Then query result
      | result              |
      | 2024-01-15 10:00:00 |

    Scenario: 7-arg make_timestamp with explicit timezone — session timezone used for display only
      # Epoch = 2024-01-15 10:00:00 UTC; displayed in NY session = 10:00 UTC - 5h = 05:00 NY
      When query
      """
      SELECT CAST(make_timestamp(2024, 1, 15, 10, 0, 0.0, 'UTC') AS STRING) AS result
      """
      Then query result
      | result              |
      | 2024-01-15 05:00:00 |

    Scenario: make_timestamp with NULL component returns NULL
      When query
      """
      SELECT make_timestamp(NULL, 1, 15, 10, 0, 0.0) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: convert_timezone returns TIMESTAMP_NTZ — the wall-clock in the target timezone

    # Spark's convert_timezone takes NTZ input and returns NTZ (not LTZ).
    # The result is the wall-clock in the target timezone; the session timezone
    # determines only the source for the 2-arg form — it does not affect the output display.

    Scenario: 2-arg form — session timezone is the source, result is UTC wall-clock
      # NTZ 10:00 in session (NY EST = UTC-5) → actual UTC 15:00 → NTZ result: 15:00
      When query
      """
      SELECT CAST(
        convert_timezone('UTC', TIMESTAMP_NTZ '2024-01-15 10:00:00')
        AS STRING
      ) AS result
      """
      Then query result
      | result              |
      | 2024-01-15 15:00:00 |

    Scenario: 3-arg form — explicit source timezone, result is target wall-clock
      # NTZ 10:00 in LA (PST = UTC-8) → actual UTC 18:00 → NTZ result: 18:00
      When query
      """
      SELECT CAST(
        convert_timezone('America/Los_Angeles', 'UTC', TIMESTAMP_NTZ '2024-01-15 10:00:00')
        AS STRING
      ) AS result
      """
      Then query result
      | result              |
      | 2024-01-15 18:00:00 |

    Scenario: convert_timezone with NULL NTZ input returns NULL
      When query
      """
      SELECT convert_timezone('UTC', CAST(NULL AS TIMESTAMP_NTZ)) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: from_utc_timestamp and to_utc_timestamp respect session timezone

    Scenario: from_utc_timestamp with NTZ input — session timezone is the implicit source
      # Spark treats the NTZ wall-clock as being in the session timezone, not UTC.
      # NTZ 10:00 in session (NY EST = UTC-5) → UTC epoch 15:00 → LTZ displayed in NY = 10:00.
      When query
      """
      SELECT CAST(
        from_utc_timestamp(TIMESTAMP_NTZ '2024-01-15 10:00:00', 'UTC')
        AS STRING
      ) AS result
      """
      Then query result
      | result              |
      | 2024-01-15 10:00:00 |

    Scenario: to_utc_timestamp with NTZ input uses explicit source timezone, session timezone affects display
      # NTZ 10:30 PST (=LA, UTC-8) → epoch = 18:30 UTC → displayed in NY session = 13:30 NY
      When query
      """
      SELECT CAST(
        to_utc_timestamp(TIMESTAMP_NTZ '2024-01-15 10:30:00', 'PST')
        AS STRING
      ) AS result
      """
      Then query result
      | result              |
      | 2024-01-15 13:30:00 |

    Scenario: from_utc_timestamp with NULL NTZ input returns NULL
      When query
      """
      SELECT CAST(
        from_utc_timestamp(CAST(NULL AS TIMESTAMP_NTZ), 'UTC')
        AS STRING
      ) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: to_utc_timestamp with NULL NTZ input returns NULL
      When query
      """
      SELECT CAST(
        to_utc_timestamp(CAST(NULL AS TIMESTAMP_NTZ), 'PST')
        AS STRING
      ) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: session timezone observes DST — offset differs between standard and daylight time

    # In July, America/New_York is EDT (UTC-4), one hour ahead of January's EST (UTC-5).

    Scenario: 7-arg make_timestamp with UTC in summer — NY is EDT (UTC-4)
      # Epoch = 2024-07-15 10:00:00 UTC; displayed in NY summer session (EDT = UTC-4) = 06:00 NY
      When query
      """
      SELECT CAST(make_timestamp(2024, 7, 15, 10, 0, 0.0, 'UTC') AS STRING) AS result
      """
      Then query result
      | result              |
      | 2024-07-15 06:00:00 |

    Scenario: to_utc_timestamp in summer — PST alias observes DST (PDT = UTC-7 in July)
      # PST maps to America/Los_Angeles; in July that is PDT (UTC-7).
      # NTZ 10:30 PDT (UTC-7) → epoch = 17:30 UTC → NY summer (EDT = UTC-4) = 13:30 NY
      When query
      """
      SELECT CAST(
        to_utc_timestamp(TIMESTAMP_NTZ '2024-07-15 10:30:00', 'PST')
        AS STRING
      ) AS result
      """
      Then query result
      | result              |
      | 2024-07-15 13:30:00 |

  Rule: make_timestamp returns TIMESTAMP_LTZ — same UTC epoch displays differently per session timezone

    # make_timestamp captures session timezone at plan time; unlike convert_timezone (which
    # returns NTZ), make_timestamp returns LTZ so the same UTC epoch renders differently
    # in each session timezone.
    # Reference: DateExpressionsSuite SPARK-51415 uses sessionZoneId from SQLConf.

    Scenario Outline: 7-arg make_timestamp with UTC — display shifts with session timezone
      Given config spark.sql.session.timeZone = <tz>
      When query
      """
      SELECT CAST(make_timestamp(2024, 1, 15, 10, 0, 0.0, 'UTC') AS STRING) AS result
      """
      Then query result
      | result   |
      | <result> |

      Examples:
        | tz                  | result              |
        | America/New_York    | 2024-01-15 05:00:00 |
        | UTC                 | 2024-01-15 10:00:00 |
        | America/Los_Angeles | 2024-01-15 02:00:00 |
