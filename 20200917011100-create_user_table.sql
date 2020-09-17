
-- +migrate Up
CREATE TABLE IF NOT EXISTS users (
    id      	INTEGER      AUTO_INCREMENT NOT NULL,
    username    VARCHAR(255)                NOT NULL,
    password	VARCHAR(255)				NOT NULL,
    PRIMARY KEY (id)
);

-- +migrate Down
DROP TABLE users;
