create table WEB_PAGE
(
    WP_WEB_PAGE_SK      INTEGER       not null,
    WP_WEB_PAGE_ID      VARCHAR(16) not null,
    WP_REC_START_DATE   DATE,
    WP_REC_END_DATE     DATE,
    WP_CREATION_DATE_SK INTEGER,
    WP_ACCESS_DATE_SK   INTEGER,
    WP_AUTOGEN_FLAG     VARCHAR,
    WP_CUSTOMER_SK      INTEGER,
    WP_URL              VARCHAR(100),
    WP_TYPE             VARCHAR(50),
    WP_CHAR_COUNT       INTEGER,
    WP_LINK_COUNT       INTEGER,
    WP_IMAGE_COUNT      INTEGER,
    WP_MAX_AD_COUNT     INTEGER,
    constraint WEB_PAGE_PK
        primary key (WP_WEB_PAGE_SK)
);
