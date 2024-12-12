create table TIME_DIM
(
    T_TIME_SK   INTEGER       not null,
    T_TIME_ID   VARCHAR(16) not null,
    T_TIME      INTEGER       not null,
    T_HOUR      INTEGER,
    T_MINUTE    INTEGER,
    T_SECOND    INTEGER,
    T_AM_PM     VARCHAR(2),
    T_SHIFT     VARCHAR(20),
    T_SUB_SHIFT VARCHAR(20),
    T_MEAL_TIME VARCHAR(20),
    constraint TIME_DIM_PK
        primary key (T_TIME_SK)
);
