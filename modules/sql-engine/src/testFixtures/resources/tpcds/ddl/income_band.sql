create table INCOME_BAND
(
    IB_INCOME_BAND_SK INTEGER not null,
    IB_LOWER_BOUND    INTEGER,
    IB_UPPER_BOUND    INTEGER,
    constraint INCOME_BAND_PK
        primary key (IB_INCOME_BAND_SK)
);
