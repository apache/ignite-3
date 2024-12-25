create table INVENTORY
(
    INV_DATE_SK          INTEGER not null,
    INV_ITEM_SK          INTEGER not null,
    INV_WAREHOUSE_SK     INTEGER not null,
    INV_QUANTITY_ON_HAND INTEGER,
    constraint INVENTORY_PK
        primary key (INV_DATE_SK, INV_ITEM_SK, INV_WAREHOUSE_SK)
);
