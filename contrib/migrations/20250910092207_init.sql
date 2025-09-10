-- +goose Up
-- +goose StatementBegin
SELECT 'Create sample table';
CREATE TABLE sample (
    id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name VARCHAR(255) NOT NULL,
    content TEXT,
    date_create TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    date_update TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX uidx__sample__name ON sample(name);
CREATE INDEX idx__sample__date_update ON sample(date_update);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'Dropping sample table';
DROP TABLE IF EXISTS sample;

-- +goose StatementEnd
