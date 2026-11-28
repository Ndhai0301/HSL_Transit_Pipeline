\set ON_ERROR_STOP on

CREATE SCHEMA raw_gtfs AUTHORIZATION pipeline;   
CREATE SCHEMA silver   AUTHORIZATION pipeline;

GRANT USAGE ON SCHEMA raw_gtfs, silver TO bi_reader;

ALTER DEFAULT PRIVILEGES FOR ROLE pipeline GRANT SELECT ON TABLES TO bi_reader;

SET ROLE pipeline;

CREATE TABLE silver.vehicle_position(
    oper   smallint   NOT NULL,
    veh    integer    NOT NULL,
    tst    timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    route   text,
    desi    text,
    dir     smallint CHECK(dir IN(1,2)),
    oday    date,
    start_time text Check(start_time ~ '^\d{2}:\d{2}$'),

    jrn integer,
    line integer,
    lat double precision,
    lon double precision,
    loc text,
    spd real,
    hdg smallint,
    acc real,
    dl integer,
    odo integer,
    drst smallint,
    occu smallint,
    next_stop text,
    PRIMARY KEY (oper, veh, tst)
);

CREATE TABLE silver.stop_event (
    event_type  text             NOT NULL CHECK (event_type IN ('ARR', 'DEP')),
    oper        smallint         NOT NULL,
    veh         integer          NOT NULL,
    tst         timestamptz      NOT NULL,
    received_at timestamptz      NOT NULL,
    route       text,
    desi        text,
    dir         smallint CHECK (dir IN (1, 2)),
    oday        date,
    start_time  text CHECK (start_time ~ '^\d{2}:\d{2}$'),
    jrn         integer,
    line        integer,
    stop_id     text             NOT NULL,  -- payload 'stop' (for DEP it differs from the topic's next_stop)
    ttarr       timestamptz,                -- scheduled arrival
    ttdep       timestamptz,                -- scheduled departure
    dl          integer,
    lat         double precision,
    lon         double precision,
    loc         text,
    odo         integer,
    drst        smallint,
    PRIMARY KEY (event_type, oper, veh, tst)
);

-- Join to GTFS trips: (oday, route, dir, start) matched 142/146 journeys in profiling
CREATE INDEX stop_event_trip_idx ON silver.stop_event (oday, route, dir, start_time);
-- Per-stop punctuality queries
CREATE INDEX stop_event_stop_idx ON silver.stop_event (stop_id, tst);

RESET ROLE;