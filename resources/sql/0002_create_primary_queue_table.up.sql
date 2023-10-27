CREATE TABLE IF NOT EXISTS schedule.primary_queue (
    id SERIAL PRIMARY KEY
    , endpoint varchar(1024)
    , headers TEXT NOT NULL
    , payload TEXT NOT NULL
    , send_after BIGINT NOT NULL
    , max_retry INT NOT NULL
    , back_off_ms INT NOT NULL
    , time_to_live BIGINT NOT NULL
    , status INT NOT NULL DEFAULT 0
);
