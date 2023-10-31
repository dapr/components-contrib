DELETE FROM hosts;
DELETE FROM reminders;

--- ---

INSERT INTO hosts
  (host_id, host_address, host_app_id, host_actors_api_level, host_last_healthcheck)
VALUES 
  ('7de434ce-e285-444f-9857-4d30cade3111', '1.1.1.1', 'myapp1', 10, CURRENT_TIMESTAMP + interval '1 day'),
  ('50d7623f-b165-4f9e-9f05-3b7a1280b222', '1.1.1.2', 'myapp2', 10, CURRENT_TIMESTAMP + interval '1 day'),
  ('ded1e507-ed4a-4322-a3a4-b5e8719a9333', '1.1.1.3', 'myapp3', 10, CURRENT_TIMESTAMP + interval '1 day');

INSERT INTO hosts_actor_types
  (host_id, actor_type, actor_idle_timeout)
VALUES
  ('7de434ce-e285-444f-9857-4d30cade3111', 'type-1', 600),
  ('7de434ce-e285-444f-9857-4d30cade3111', 'type-2', 600),
  ('50d7623f-b165-4f9e-9f05-3b7a1280b222', 'type-1', 600),
  ('50d7623f-b165-4f9e-9f05-3b7a1280b222', 'type-2', 600),
  ('ded1e507-ed4a-4322-a3a4-b5e8719a9333', 'type-3', 600);

INSERT INTO actors
  (actor_type, actor_id, host_id, actor_idle_timeout, actor_activation)
VALUES
  ('type-1', 'actor-1.1', '7de434ce-e285-444f-9857-4d30cade3111', 600, CURRENT_TIMESTAMP),
  ('type-1', 'actor-1.2', '50d7623f-b165-4f9e-9f05-3b7a1280b222', 600, CURRENT_TIMESTAMP - interval '2 second'),
  ('type-2', 'actor-2.1', '7de434ce-e285-444f-9857-4d30cade3111', 600, CURRENT_TIMESTAMP - interval '10 second'),
  ('type-2', 'actor-2.2', '50d7623f-b165-4f9e-9f05-3b7a1280b222', 600, CURRENT_TIMESTAMP - interval '15 second'),
  ('type-2', 'actor-2.3', '50d7623f-b165-4f9e-9f05-3b7a1280b222', 600, CURRENT_TIMESTAMP - interval '1 minute'),
  ('type-3', 'actor-3.1', 'ded1e507-ed4a-4322-a3a4-b5e8719a9333', 600, CURRENT_TIMESTAMP - interval '10 minute');

INSERT INTO reminders
  (reminder_id, actor_type, actor_id, reminder_name, reminder_execution_time, reminder_period)
VALUES
  ('0484b922-7ce5-4889-9507-ebccdbaeffc1', 'type-1', 'actor-1.1', 'reminder-1.1.1', CURRENT_TIMESTAMP + interval '12 hour', NULL),
  ('00f2fef6-8d9b-4a5f-a971-e85e17036f84', 'type-1', 'actor-1.1', 'reminder-1.1.2', CURRENT_TIMESTAMP + interval '6 hour', NULL),
  ('d8b7e84c-2fb7-40cb-8d8b-559ddbaa537b', 'type-1', 'actor-1.1', 'reminder-1.1.3', CURRENT_TIMESTAMP + interval '8 hour', NULL),
  ('9245f9aa-c48d-46f7-b285-2b82fb27c8a5', 'type-1', 'actor-1.2', 'reminder-1.2.1', CURRENT_TIMESTAMP + interval '12 hour', NULL),
  ('f5ddc197-564a-41dd-a8a0-23b387abc1c6', 'type-1', 'actor-1.2', 'reminder-1.2.2', CURRENT_TIMESTAMP + interval '6 hour', NULL),
  ('da0a622f-fa0b-4846-9672-d8d83cbdf64c', 'type-1', 'actor-1.2', 'reminder-1.2.3', CURRENT_TIMESTAMP + interval '8 hour', '2h'),
  ('8b802e3d-c7e2-443e-9cf4-33720f145876', 'type-1', 'actor-1.inactive', 'reminder-1.inactive.3', CURRENT_TIMESTAMP + interval '6 hour', '2h'),
  ('490eee6f-f71b-4b52-bfdf-2784fc427854', 'type-2', 'actor-2.1', 'reminder-2.1.1', CURRENT_TIMESTAMP + interval '8 hour', NULL),
  ('3357df73-5315-49d1-9a49-3290d378067f', 'type-2', 'actor-2.1', 'reminder-2.1.2', CURRENT_TIMESTAMP + interval '8 hour', '1h'),
  ('16b3c3a1-0a50-48dd-8ebb-fc6361af427e', 'type-3', 'actor-3.inactive', 'reminder-3.inactive.1', CURRENT_TIMESTAMP + interval '6 hour', '30m'),
  ('b2670bca-55e4-4451-8adf-9c4cdde3409b', 'type-3', 'actor-3.inactive', 'reminder-3.inactive.2', CURRENT_TIMESTAMP + interval '6 hour', '30m');

--- ---

DEALLOCATE myquery;
PREPARE myquery (text, text, text, interval, text, timestamp with time zone, bytea, text, text[], uuid[]) AS

WITH c AS (
    SELECT
        CURRENT_TIMESTAMP AS reminder_lease_time,
        $8 AS reminder_lease_pid
    FROM actors
    WHERE
        actor_type = $1
        AND actor_id = $2
        AND (
            (
                host_id IS NULL
                AND actor_type = ANY($9)
            )
            OR host_id = ANY($10)
        )
), lease AS (
    SELECT
        c.reminder_lease_time,
        c.reminder_lease_pid
    FROM c
    UNION ALL
        SELECT
            NULL AS reminder_lease_time,
            NULL AS reminder_lease_pid
        WHERE NOT EXISTS (
            SELECT 1 from c
        )
)
INSERT INTO reminders
      (actor_type, actor_id, reminder_name, reminder_execution_time, reminder_period, reminder_ttl, reminder_data, reminder_lease_time, reminder_lease_pid)
    SELECT
      $1, $2, $3, CURRENT_TIMESTAMP + $4::interval, $5, $6, $7, lease.reminder_lease_time, lease.reminder_lease_pid
    FROM lease
    ON CONFLICT (actor_type, actor_id, reminder_name) DO UPDATE SET
      reminder_execution_time = EXCLUDED.reminder_execution_time,
      reminder_period = EXCLUDED.reminder_period,
      reminder_ttl = EXCLUDED.reminder_ttl,
      reminder_data = EXCLUDED.reminder_data,
      reminder_lease_time = EXCLUDED.reminder_lease_time,
      reminder_lease_pid = EXCLUDED.reminder_lease_pid
    RETURNING reminder_id, actor_type, actor_id, reminder_name,
      EXTRACT(EPOCH FROM reminder_execution_time - CURRENT_TIMESTAMP)::int,
      reminder_lease_time;

EXECUTE myquery ('type-1', 'actor-1.1', 'mytestreminder', '1 hour', NULL, NULL, NULL, 'mypid', '{type-1,type-2}'::text[], '{7de434ce-e285-444f-9857-4d30cade3111}'::uuid[]);


--- ---

DEALLOCATE myquery;
PREPARE myquery (text, text, text, interval, text, timestamp with time zone, bytea, text, text[], uuid[]) AS

WITH lease AS (
    SELECT
        CURRENT_TIMESTAMP AS reminder_lease_time,
        $8 AS reminder_lease_pid
    FROM actors
    WHERE
        actor_type = $1
        AND actor_id = $2
        AND (
            (
                host_id IS NULL
                AND actor_type = ANY($9)
            )
            OR host_id = ANY($10)
        )
)
INSERT INTO reminders
      (actor_type, actor_id, reminder_name, reminder_execution_time, reminder_period, reminder_ttl, reminder_data, reminder_lease_time, reminder_lease_pid)
    SELECT
      $1, $2, $3, CURRENT_TIMESTAMP + $4::interval, $5, $6, $7, lease.reminder_lease_time, lease.reminder_lease_pid
    FROM lease
    ON CONFLICT (actor_type, actor_id, reminder_name) DO UPDATE SET
      reminder_execution_time = EXCLUDED.reminder_execution_time,
      reminder_period = EXCLUDED.reminder_period,
      reminder_ttl = EXCLUDED.reminder_ttl,
      reminder_data = EXCLUDED.reminder_data,
      reminder_lease_time = EXCLUDED.reminder_lease_time,
      reminder_lease_pid = EXCLUDED.reminder_lease_pid
    RETURNING reminder_id, actor_type, actor_id, reminder_name,
      EXTRACT(EPOCH FROM reminder_execution_time - CURRENT_TIMESTAMP)::int,
      reminder_lease_time;

EXECUTE myquery ('type-1', 'actor-1.1', 'mytestreminder', '1 hour', NULL, NULL, NULL, 'mypid', '{type-1,type-2}'::text[], '{7de434ce-e285-444f-9857-4d30cade3111}'::uuid[]);


--- ---

UPDATE reminders SET reminder_lease_time = NULL;

DEALLOCATE myquery;
PREPARE myquery (interval, interval, int) AS


UPDATE reminders
    SET reminder_lease_time = CURRENT_TIMESTAMP
    WHERE reminder_id IN (
        SELECT reminder_id
        FROM reminders
        WHERE 
            reminder_execution_time < CURRENT_TIMESTAMP + $1::interval
            AND (reminder_lease_time IS NULL OR reminder_lease_time < CURRENT_TIMESTAMP - $2::interval)
        ORDER BY reminder_execution_time ASC
        LIMIT $3
    )
    RETURNING
        actor_type, actor_id, reminder_name,
        EXTRACT(EPOCH FROM reminder_execution_time - CURRENT_TIMESTAMP)::int,
        reminder_lease_time;


EXECUTE myquery ('1 day', '5 minute', 3);

--- ---

UPDATE reminders SET reminder_lease_time = NULL;

DEALLOCATE myquery;
PREPARE myquery (interval, interval, text[], uuid[], int, text) AS


UPDATE reminders
    SET
        reminder_lease_time = CURRENT_TIMESTAMP,
        reminder_lease_pid = $6
    WHERE reminder_id IN (
        SELECT reminder_id
        FROM reminders
        LEFT JOIN actors
            ON actors.actor_type = reminders.actor_type AND actors.actor_id = reminders.actor_id
        WHERE 
            reminders.reminder_execution_time < CURRENT_TIMESTAMP + $1::interval
            AND (
                reminders.reminder_lease_time IS NULL
                OR reminders.reminder_lease_time < CURRENT_TIMESTAMP - $2::interval
            )
            AND (
                (
                    actors.host_id IS NULL
                    AND reminders.actor_type = ANY($3)
                )
                OR actors.host_id = ANY($4)
            )
        ORDER BY reminders.reminder_execution_time ASC
        LIMIT $5
    )
    RETURNING
        reminder_id, actor_type, actor_id, reminder_name,
        EXTRACT(EPOCH FROM reminder_execution_time - CURRENT_TIMESTAMP)::int,
        reminder_lease_time;


EXECUTE myquery ('1 day', '5 minute', '{type-1,type-2}'::text[], '{7de434ce-e285-444f-9857-4d30cade3111}'::uuid[], 10, 'mypid');

--- ---

DEALLOCATE myquery;
PREPARE myquery (text, text, interval, uuid[]) AS


WITH new_row AS (
  INSERT INTO actors (actor_type, actor_id, host_id, actor_idle_timeout, actor_activation)
    SELECT $1, $2, hosts_actor_types.host_id, hosts_actor_types.actor_idle_timeout, CURRENT_TIMESTAMP
      FROM hosts_actor_types, hosts
      WHERE
        hosts_actor_types.actor_type = $1
        AND hosts.host_id = hosts_actor_types.host_id
        AND hosts.host_id = ANY($4)
        AND NOT EXISTS (
          SELECT 1 FROM actors WHERE actor_type = $1 AND actor_id = $2
        )
      ORDER BY random() LIMIT 1
    RETURNING host_id, actor_idle_timeout
)
(
  SELECT hosts.host_id, hosts.host_app_id, hosts.host_address, actors.actor_idle_timeout
    FROM actors, hosts
    WHERE
      actors.actor_type = $1
      AND actors.actor_id = $2
      AND actors.host_id = hosts.host_id
      AND hosts.host_id = ANY($4)
      AND hosts.host_last_healthcheck >= CURRENT_TIMESTAMP - $3::interval
  UNION ALL
  SELECT hosts.host_id, hosts.host_app_id, hosts.host_address, new_row.actor_idle_timeout
    FROM new_row, hosts
    WHERE
      new_row.host_id = hosts.host_id
      AND hosts.host_last_healthcheck >= CURRENT_TIMESTAMP - $3::interval
) LIMIT 1;


EXECUTE myquery ('type-1', 'actor-1', '10 minutes', '{7de434ce-e285-444f-9857-4d30cade3111}'::uuid[]);


--- ---

DEALLOCATE myquery;
PREPARE myquery (text, text, interval) AS


WITH new_row AS (
  INSERT INTO actors (actor_type, actor_id, host_id, actor_idle_timeout, actor_activation)
    SELECT $1, $2, hosts_actor_types.host_id, hosts_actor_types.actor_idle_timeout, CURRENT_TIMESTAMP
      FROM hosts_actor_types, hosts
      WHERE
        hosts_actor_types.actor_type = $1 AND
        hosts.host_id = hosts_actor_types.host_id AND
        NOT EXISTS (
          SELECT 1 FROM actors WHERE actor_type = $1 AND actor_id = $2
        )
      ORDER BY random() LIMIT 1
    RETURNING host_id, actor_idle_timeout
)
(
  SELECT hosts.host_id, hosts.host_app_id, hosts.host_address, actors.actor_idle_timeout
    FROM actors, hosts
    WHERE
      actors.actor_type = $1
      AND actors.actor_id = $2
      AND actors.host_id = hosts.host_id
      AND hosts.host_last_healthcheck >= CURRENT_TIMESTAMP - $3::interval
  UNION ALL
  SELECT hosts.host_id, hosts.host_app_id, hosts.host_address, new_row.actor_idle_timeout
    FROM new_row, hosts
    WHERE
      new_row.host_id = hosts.host_id
      AND hosts.host_last_healthcheck >= CURRENT_TIMESTAMP - $3::interval
) LIMIT 1;


EXECUTE myquery ('type-1', 'actor-1', '10 minutes');

--- ---

DEALLOCATE myquery;
PREPARE myquery (text, text) AS


WITH new_row AS (
  INSERT INTO actors (actor_type, actor_id, host_id, actor_idle_timeout, actor_activation)
    SELECT $1, $2, hosts_actor_types.host_id, hosts_actor_types.actor_idle_timeout, CURRENT_TIMESTAMP
      FROM hosts_actor_types, hosts
      WHERE
        hosts_actor_types.actor_type = $1
        AND hosts.host_id = hosts_actor_types.host_id
        AND hosts.host_last_healthcheck >= CURRENT_TIMESTAMP - $3::interval
        AND NOT EXISTS (
          SELECT actors.host_id
            FROM actors, hosts
            WHERE
              actors.actor_type = $1
              AND actors.actor_id = $2
              AND actors.host_id = hosts.host_id
              AND hosts.host_last_healthcheck >= CURRENT_TIMESTAMP - $3::interval
        )
      ORDER BY random() LIMIT 1
    ON CONFLICT (actor_type, actor_id) DO UPDATE
      SET
        host_id = EXCLUDED.host_id, actor_idle_timeout = EXCLUDED.actor_idle_timeout, actor_activation = EXCLUDED.actor_activation
    RETURNING host_id, actor_idle_timeout
)
(
  SELECT hosts.host_id, hosts.host_app_id, hosts.host_address, actors.actor_idle_timeout
    FROM actors, hosts
    WHERE
      actors.actor_type = $1
      AND actors.actor_id = $2
      AND actors.host_id = hosts.host_id
      AND hosts.host_last_healthcheck >= CURRENT_TIMESTAMP - $3::interval
  UNION ALL
  SELECT hosts.host_id, hosts.host_app_id, hosts.host_address, new_row.actor_idle_timeout
    FROM new_row, hosts
    WHERE
      new_row.host_id = hosts.host_id
      AND hosts.host_last_healthcheck >= CURRENT_TIMESTAMP - $3::interval
) LIMIT 1;


EXECUTE myquery ('type-1', 'actor-1', '10 minutes');
