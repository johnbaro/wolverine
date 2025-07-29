create table wolverine_outgoing_envelopes
(
    id           uuid    not null
        constraint pkey_wolverine_outgoing_envelopes_id
            primary key,
    owner_id     integer not null,
    destination  varchar not null,
    deliver_by   timestamp with time zone,
    body         bytea   not null,
    attempts     integer default 0,
    message_type varchar not null
);

alter table wolverine_outgoing_envelopes
    owner to postgres;

create table wolverine_incoming_envelopes
(
    id             uuid    not null
        constraint pkey_wolverine_incoming_envelopes_id
            primary key,
    status         varchar not null,
    owner_id       integer not null,
    execution_time timestamp with time zone,
    attempts       integer default 0,
    body           bytea   not null,
    message_type   varchar not null,
    received_at    varchar,
    keep_until     timestamp with time zone
);

alter table wolverine_incoming_envelopes
    owner to postgres;

create table wolverine_dead_letters
(
    id                uuid    not null
        constraint pkey_wolverine_dead_letters_id
            primary key,
    execution_time    timestamp with time zone,
    body              bytea   not null,
    message_type      varchar not null,
    received_at       varchar,
    source            varchar,
    exception_type    varchar,
    exception_message varchar,
    sent_at           timestamp with time zone,
    replayable        boolean
);

alter table wolverine_dead_letters
    owner to postgres;

create table wolverine_nodes
(
    id           uuid                                   not null
        constraint pkey_wolverine_nodes_id
            primary key,
    node_number  serial,
    description  varchar                                not null,
    uri          varchar                                not null,
    started      timestamp with time zone default now() not null,
    health_check timestamp with time zone default now() not null,
    version      varchar,
    capabilities text[]
);

alter table wolverine_nodes
    owner to postgres;

create table wolverine_node_assignments
(
    id      varchar                                not null
        constraint pkey_wolverine_node_assignments_id
            primary key,
    node_id uuid
        constraint fkey_wolverine_node_assignments_node_id
            references wolverine_nodes
            on delete cascade,
    started timestamp with time zone default now() not null
);

alter table wolverine_node_assignments
    owner to postgres;

create table wolverine_control_queue
(
    id           uuid                                   not null
        constraint pkey_wolverine_control_queue_id
            primary key,
    message_type varchar                                not null,
    node_id      uuid                                   not null,
    body         bytea                                  not null,
    posted       timestamp with time zone default now() not null,
    expires      timestamp with time zone
);

alter table wolverine_control_queue
    owner to postgres;

create table wolverine_node_records
(
    id          serial
        constraint pkey_wolverine_node_records_id
            primary key,
    node_number integer                                not null,
    event_name  varchar                                not null,
    timestamp   timestamp with time zone default now() not null,
    description varchar
);

alter table wolverine_node_records
    owner to postgres;

