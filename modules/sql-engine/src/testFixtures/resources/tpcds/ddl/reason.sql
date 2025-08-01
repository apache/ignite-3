create table REASON
(
    R_REASON_SK   INTEGER not null,
    R_REASON_ID   VARCHAR(16),
    R_REASON_DESC VARCHAR(100),
    constraint REASON_PK
        primary key (R_REASON_SK)
);
