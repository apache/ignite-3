create table ITEM
(
    I_ITEM_SK        INTEGER       not null,
    I_ITEM_ID        VARCHAR(16) not null,
    I_REC_START_DATE DATE,
    I_REC_END_DATE   DATE,
    I_ITEM_DESC      VARCHAR(200),
    I_CURRENT_PRICE  NUMERIC(7, 2),
    I_WHOLESALE_COST NUMERIC(7, 2),
    I_BRAND_ID       INTEGER,
    I_BRAND          VARCHAR(50),
    I_CLASS_ID       INTEGER,
    I_CLASS          VARCHAR(50),
    I_CATEGORY_ID    INTEGER,
    I_CATEGORY       VARCHAR(50),
    I_MANUFACT_ID    INTEGER,
    I_MANUFACT       VARCHAR(50),
    I_SIZE           VARCHAR(20),
    I_FORMULATION    VARCHAR(20),
    I_COLOR          VARCHAR(20),
    I_UNITS          VARCHAR(10),
    I_CONTAINER      VARCHAR(10),
    I_MANAGER_ID     INTEGER,
    I_PRODUCT_NAME   VARCHAR(50),
    constraint ITEM_PK
        primary key (I_ITEM_SK)
);
