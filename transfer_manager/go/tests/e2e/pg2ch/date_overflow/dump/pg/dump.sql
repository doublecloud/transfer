CREATE TABLE "invoice"
(
    "invoice_id" INT NOT NULL,
    "customer_id" INT NOT NULL,
    "invoice_date" TIMESTAMP NOT NULL,
    "billing_address" VARCHAR(70),
    "billing_city" VARCHAR(40),
    "billing_state" VARCHAR(40),
    "billing_country" VARCHAR(40),
    "billing_postal_code" VARCHAR(10),
    "total" NUMERIC(10,2) NOT NULL,
    CONSTRAINT "PK_Invoice" PRIMARY KEY  ("invoice_id")
);

insert into public.invoice (invoice_id, customer_id, invoice_date, billing_address, billing_city, billing_state, billing_country, billing_postal_code, total) values (1, 1, '0001-01-01 00:00:00.000000', null, null, null, null, null, 492.00);
