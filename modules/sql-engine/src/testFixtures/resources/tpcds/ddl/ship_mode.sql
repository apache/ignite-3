create table SHIP_MODE
(
    SM_SHIP_MODE_SK INTEGER not null,
    SM_SHIP_MODE_ID VARCHAR(16),
    SM_TYPE         VARCHAR(30),
    SM_CODE         VARCHAR(10),
    SM_CARRIER      VARCHAR(20),
    SM_CONTRACT     VARCHAR(20),
    constraint SHIP_MODE_PK
        primary key (SM_SHIP_MODE_SK)
);
