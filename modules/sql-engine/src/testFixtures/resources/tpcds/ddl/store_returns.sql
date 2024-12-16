create table STORE_RETURNS
(
    SR_RETURNED_DATE_SK   INTEGER,
    SR_RETURN_TIME_SK     INTEGER,
    SR_ITEM_SK            INTEGER not null,
    SR_CUSTOMER_SK        INTEGER,
    SR_CDEMO_SK           INTEGER,
    SR_HDEMO_SK           INTEGER,
    SR_ADDR_SK            INTEGER,
    SR_STORE_SK           INTEGER,
    SR_REASON_SK          INTEGER,
    SR_TICKET_NUMBER      INTEGER not null,
    SR_RETURN_QUANTITY    INTEGER,
    SR_RETURN_AMT         NUMERIC(7, 2),
    SR_RETURN_TAX         NUMERIC(7, 2),
    SR_RETURN_AMT_INC_TAX NUMERIC(7, 2),
    SR_FEE                NUMERIC(7, 2),
    SR_RETURN_SHIP_COST   NUMERIC(7, 2),
    SR_REFUNDED_CASH      NUMERIC(7, 2),
    SR_REVERSED_CHARGE    NUMERIC(7, 2),
    SR_STORE_CREDIT       NUMERIC(7, 2),
    SR_NET_LOSS           NUMERIC(7, 2),
    constraint SR_ITEM_SK
        primary key (SR_ITEM_SK, SR_TICKET_NUMBER)
);
