create table STORE_SALES
(
    SS_SOLD_DATE_SK       INTEGER,
    SS_SOLD_TIME_SK       INTEGER,
    SS_ITEM_SK            INTEGER not null,
    SS_CUSTOMER_SK        INTEGER,
    SS_CDEMO_SK           INTEGER,
    SS_HDEMO_SK           INTEGER,
    SS_ADDR_SK            INTEGER,
    SS_STORE_SK           INTEGER,
    SS_PROMO_SK           INTEGER,
    SS_TICKET_NUMBER      INTEGER not null,
    SS_QUANTITY           INTEGER,
    SS_WHOLESALE_COST     NUMERIC(7, 2),
    SS_LIST_PRICE         NUMERIC(7, 2),
    SS_SALES_PRICE        NUMERIC(7, 2),
    SS_EXT_DISCOUNT_AMT   NUMERIC(7, 2),
    SS_EXT_SALES_PRICE    NUMERIC(7, 2),
    SS_EXT_WHOLESALE_COST NUMERIC(7, 2),
    SS_EXT_LIST_PRICE     NUMERIC(7, 2),
    SS_EXT_TAX            NUMERIC(7, 2),
    SS_COUPON_AMT         NUMERIC(7, 2),
    SS_NET_PAID           NUMERIC(7, 2),
    SS_NET_PAID_INC_TAX   NUMERIC(7, 2),
    SS_NET_PROFIT         NUMERIC(7, 2),
    constraint SS_ITEM_SK
        primary key (SS_ITEM_SK, SS_TICKET_NUMBER)
);
