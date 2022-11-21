create table banks(
	bban VARCHAR(18) NOT NULL,
	swift VARCHAR(8) NOT NULL,
	name VARCHAR(40),
	address VARCHAR(80),
	phone INT,
	PRIMARY KEY ( bban )
);

\d banks;

create table customers(
	bban VARCHAR(18) NOT NULL,
	customerID VARCHAR(36) NOT NULL,
	firstName VARCHAR(40),
	lastName VARCHAR(40),
	city VARCHAR(40),
	phone INT,
	PRIMARY KEY ( customerID )
);

\d customers;

create table accounts(
	bban VARCHAR(18) NOT NULL,
	customerID VARCHAR(36) NOT NULL,
	accountID VARCHAR(36) NOT NULL,
	type VARCHAR(8) NOT NULL,
	balance DECIMAL(10,2),
	status VARCHAR(6),
	PRIMARY KEY ( accountID )
);

\d accounts;

create table transactions(
	accountID VARCHAR(36) NOT NULL,
	type VARCHAR(6) NOT NULL,
	amount DECIMAL(10,2),
	industry VARCHAR(80),
	PRIMARY KEY ( accountID )
);

\d transactions;
\dt;
