CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(29) PRIMARY KEY,
    transaction_date DATE,
    product_id INT, 
    account_id INT,
    quantity INT,

    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);