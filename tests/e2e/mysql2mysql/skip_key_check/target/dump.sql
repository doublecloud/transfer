CREATE TABLE Persons (
    PersonID int NOT NULL,
    LastName varchar(255) NOT NULL,
    FirstName varchar(255),
    FavouriteOrderID int,
    PRIMARY KEY (PersonID)
);

CREATE TABLE Orders (
    OrderID int NOT NULL,
    OrderNumber int NOT NULL,
    PersonID int,
    PRIMARY KEY (OrderID),
    FOREIGN KEY (PersonID) REFERENCES Persons(PersonID)
);

INSERT INTO 
    Persons (PersonID, LastName, FirstName, FavouriteOrderID) 
VALUES 
    (1, "Maria", "Ivanova", 1),
    (2, "Maxim", "Petrov", 4);

INSERT INTO Orders 
    (OrderID, OrderNumber, PersonID) 
VALUES 
    (1, 1, 1),
    (2, 2, 1),
    (3, 3, 1),
    (4, 4, 2),
    (5, 5, 2);

ALTER TABLE Persons ADD CONSTRAINT fk1
  FOREIGN KEY (FavouriteOrderID)
  REFERENCES Orders(OrderID);

