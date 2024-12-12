create table WEB_SALES
(
    WS_SOLD_DATE_SK          INTEGER,
    WS_SOLD_TIME_SK          INTEGER,
    WS_SHIP_DATE_SK          INTEGER,
    WS_ITEM_SK               INTEGER not null,
    WS_BILL_CUSTOMER_SK      INTEGER,
    WS_BILL_CDEMO_SK         INTEGER,
    WS_BILL_HDEMO_SK         INTEGER,
    WS_BILL_ADDR_SK          INTEGER,
    WS_SHIP_CUSTOMER_SK      INTEGER,
    WS_SHIP_CDEMO_SK         INTEGER,
    WS_SHIP_HDEMO_SK         INTEGER,
    WS_SHIP_ADDR_SK          INTEGER,
    WS_WEB_PAGE_SK           INTEGER,
    WS_WEB_SITE_SK           INTEGER,
    WS_SHIP_MODE_SK          INTEGER,
    WS_WAREHOUSE_SK          INTEGER,
    WS_PROMO_SK              INTEGER,
    WS_ORDER_NUMBER          INTEGER not null,
    WS_QUANTITY              INTEGER,
    WS_WHOLESALE_COST        NUMERIC(7, 2),
    WS_LIST_PRICE            NUMERIC(7, 2),
    WS_SALES_PRICE           NUMERIC(7, 2),
    WS_EXT_DISCOUNT_AMT      NUMERIC(7, 2),
    WS_EXT_SALES_PRICE       NUMERIC(7, 2),
    WS_EXT_WHOLESALE_COST    NUMERIC(7, 2),
    WS_EXT_LIST_PRICE        NUMERIC(7, 2),
    WS_EXT_TAX               NUMERIC(7, 2),
    WS_COUPON_AMT            NUMERIC(7, 2),
    WS_EXT_SHIP_COST         NUMERIC(7, 2),
    WS_NET_PAID              NUMERIC(7, 2),
    WS_NET_PAID_INC_TAX      NUMERIC(7, 2),
    WS_NET_PAID_INC_SHIP     NUMERIC(7, 2),
    WS_NET_PAID_INC_SHIP_TAX NUMERIC(7, 2),
    WS_NET_PROFIT            NUMERIC(7, 2),
    constraint WEB_SALES_PK
        primary key (WS_ITEM_SK, WS_ORDER_NUMBER)
);
