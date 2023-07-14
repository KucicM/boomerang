CREATE TABLE IF NOT EXISTS PriorityQueue (
    id TEXT PRIMARY KEY, 
    endpoint TEXT,
    sendAfter BIGINT
);
