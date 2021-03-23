
--- create the product description table
create table prod_desc(
    sku character varying(255),
    name character varying(255),
    description character varying(255),
    PRIMARY KEY(sku, name));

COMMENT ON TABLE prod_desc IS 'table to contain the product description';

COMMENT ON COLUMN prod_desc.sku IS 'sku that the product belongs to';
COMMENT ON COLUMN prod_desc.name IS 'name of the product';
COMMENT ON COLUMN prod_desc.description IS 'description of the product';

--- create product count table
create table prod_count(
    name character varying(255),
    no_of_products integer);

COMMENT ON TABLE prod_count IS 'table to contain the product count';

COMMENT ON COLUMN prod_count.name IS 'name of the product';
COMMENT ON COLUMN prod_count.no_of_products IS 'number of products';
