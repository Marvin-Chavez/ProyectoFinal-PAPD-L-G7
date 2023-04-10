CREATE_DW = '''
CREATE TABLE IF NOT EXISTS dimDate (
    order_date DATE NOT NULL,
    year INT(4) NOT NULL,
    month INT(2) NOT NULL,
    quarter INT(1) NOT NULL,
    day INT(2) NOT NULL,
    week INT(2) NOT NULL,
    dayofweek INT(1) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    order_date_id INT NOT NULL,
    PRIMARY KEY (order_date_id)
);

CREATE TABLE IF NOT EXISTS dimStatus (
    status_id INT NOT NULL,
    status VARCHAR(255) NOT NULL,
    PRIMARY KEY (status_id)
);

CREATE TABLE IF NOT EXISTS dimProduct (
    item_id INT NOT NULL,
    sku VARCHAR(255) NOT NULL,
    category_id INT(11) NOT NULL,
    category VARCHAR(255) NOT NULL,
    PRIMARY KEY (item_id)
);

CREATE TABLE IF NOT EXISTS dimPaymentMethod (
    payment_method_id INT NOT NULL,
    payment_method VARCHAR(255) NOT NULL,
    PRIMARY KEY (payment_method_id)
);

CREATE TABLE IF NOT EXISTS dimCust (
    cust_id INT NOT NULL,
    ref_num INT,
    name_prefix VARCHAR(10),
    first_name VARCHAR(255),
    middle_initial CHAR(1),
    last_name VARCHAR(255),
    gender CHAR(1),
    age INT,
    full_name VARCHAR(255),
    email VARCHAR(255),
    customer_since DATE,
    ssn VARCHAR(11),
    phone_no VARCHAR(20),
    user_name VARCHAR(255),
    PRIMARY KEY (cust_id)
);

CREATE TABLE IF NOT EXISTS dimAddress (
    direccion_id INT NOT NULL,
    place_name VARCHAR(255),
    county VARCHAR(255),
    city VARCHAR(255),
    state CHAR(2),
    zip VARCHAR(10),
    region VARCHAR(255),
    PRIMARY KEY (direccion_id)
);

CREATE TABLE IF NOT EXISTS Factable (
    order_id INT NOT NULL,
    qty_ordered INT,
    price DECIMAL(10, 2),
    value DECIMAL(10, 2),
    discount_amount DECIMAL(10, 2),
    total DECIMAL(10, 2),
    discount_percent DECIMAL(5, 2),
    order_date_id INT,
    order_date DATE,
    status_id INT,
    item_id INT,
    payment_method_id INT,
    cust_id INT,
    direccion_id INT,
    PRIMARY KEY (order_id),
    CONSTRAINT fk_order_date_id FOREIGN KEY (order_date_id) REFERENCES dimDate(order_date_id),
    CONSTRAINT fk_status_id FOREIGN KEY (status_id) REFERENCES dimStatus(status_id),
    CONSTRAINT fk_item_id FOREIGN KEY (item_id) REFERENCES dimProduct(item_id),
    CONSTRAINT fk_payment_method_id FOREIGN KEY (payment_method_id) REFERENCES dimPaymentMethod(payment_method_id),
    CONSTRAINT fk_cust_id FOREIGN KEY (cust_id) REFERENCES dimCust(cust_id),
    CONSTRAINT fk_direccion_id FOREIGN KEY (direccion_id) REFERENCES dimAddress(direccion_id)
);

'''