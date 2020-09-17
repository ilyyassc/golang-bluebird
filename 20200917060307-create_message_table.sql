
-- +migrate Up
CREATE TABLE IF NOT EXISTS messages (
    id      	INTEGER      AUTO_INCREMENT NOT NULL,
    queue_id	INTEGER						NOT NULL,
    message	    VARCHAR(255)                NOT NULL,
    PRIMARY KEY (id)
);

-- +migrate Down
DROP TABLE users;