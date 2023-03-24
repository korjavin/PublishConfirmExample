ALTER SYSTEM SET max_prepared_transactions = 100;

CREATE TABLE messages (
    id SERIAL PRIMARY KEY,
    message TEXT,
    is_sent BOOLEAN DEFAULT false
);

insert into messages (id,message) values (1,'Hello World'), (2,'Hello World 2'), (3,'Hello World 3');
